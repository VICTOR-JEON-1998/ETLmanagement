"""sudo를 사용하여 Jobs 디렉토리 내용 확인"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.ssh_client import DataStageSSHClient

def find_jobs_with_sudo():
    """sudo를 사용하여 Jobs 디렉토리 내용 확인"""
    client = DataStageSSHClient()
    
    if not client.connect():
        print("SSH 연결 실패")
        return
    
    print("=" * 70)
    print("sudo를 사용한 Jobs 디렉토리 내용 확인")
    print("=" * 70)
    
    project_paths = [
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM",
        "/home/dsadm/Projects/BIDW_ADM",
    ]
    
    all_jobs = []
    
    for project_path in project_paths:
        jobs_path = f"{project_path}/Jobs"
        
        print(f"\n{'='*70}")
        print(f"경로: {jobs_path}")
        print(f"{'='*70}")
        
        # Jobs 디렉토리 존재 확인
        check_result = client.execute_command(f"test -d '{jobs_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        if "EXISTS" not in check_result["stdout"]:
            print("  ❌ Jobs 디렉토리 없음")
            continue
        
        print("  ✅ Jobs 디렉토리 존재")
        
        # sudo를 사용하여 내용 확인
        print("\n  [sudo -i로 내용 확인]")
        list_cmd = f"ls -la '{jobs_path}' 2>/dev/null | head -100"
        result = client.execute_command(list_cmd, use_sudo=True)
        
        if result["stdout"].strip():
            print("  ✅ 내용 확인 성공:")
            lines = result["stdout"].strip().split('\n')
            print(f"  총 {len(lines)}개 항목")
            
            for line in lines[:50]:
                if line.strip() and not line.strip().startswith('total'):
                    print(f"    {line.strip()}")
                    # 디렉토리 이름 추출 (Job 이름일 수 있음)
                    parts = line.strip().split()
                    if len(parts) >= 9:
                        item_name = parts[-1]
                        if item_name not in ['.', '..'] and item_name not in [j.get("name") for j in all_jobs]:
                            all_jobs.append({
                                "name": item_name,
                                "path": f"{jobs_path}/{item_name}",
                                "source": "jobs_directory"
                            })
        else:
            print("  ❌ 내용을 읽을 수 없음")
            if result["stderr"]:
                print(f"    오류: {result['stderr']}")
        
        # 하위 디렉토리 확인
        print("\n  [하위 디렉토리 확인]")
        find_cmd = f"find '{jobs_path}' -maxdepth 2 -type d 2>/dev/null | head -50"
        result = client.execute_command(find_cmd, use_sudo=True)
        
        if result["stdout"].strip():
            dirs = result["stdout"].strip().split('\n')
            print(f"  총 {len(dirs)}개 디렉토리")
            for dir_path in dirs[:30]:
                if dir_path.strip() and dir_path != jobs_path:
                    print(f"    {dir_path.strip()}")
                    # Job 이름 추출
                    job_name = Path(dir_path.strip()).name
                    if job_name and job_name not in [j.get("name") for j in all_jobs]:
                        all_jobs.append({
                            "name": job_name,
                            "path": dir_path.strip(),
                            "source": "jobs_subdirectory"
                        })
        
        # 파일 검색
        print("\n  [파일 검색]")
        files_cmd = f"find '{jobs_path}' -type f 2>/dev/null | head -50"
        result = client.execute_command(files_cmd, use_sudo=True)
        
        if result["stdout"].strip():
            files = result["stdout"].strip().split('\n')
            print(f"  총 {len(files)}개 파일")
            for file_path in files[:20]:
                if file_path.strip():
                    print(f"    {file_path.strip()}")
    
    # dsadm 계정으로 dsjob 실행 시도
    print("\n" + "=" * 70)
    print("dsadm 계정으로 dsjob 실행 시도")
    print("=" * 70)
    
    dsenv_path = "/opt/IBM/InformationServer/Server/DSEngine/dsenv"
    
    # sudo -u dsadm로 실행
    dsjob_cmd = f"sudo -u dsadm bash -c 'source {dsenv_path} 2>/dev/null; dsjob -listjobs BIDW_ADM 2>&1'"
    result = client.execute_command(dsjob_cmd)
    
    if result["exit_status"] == 0 and result["stdout"].strip():
        print("  ✅ dsjob 명령어 실행 성공:")
        for line in result["stdout"].strip().split('\n')[:30]:
            if line.strip() and not line.strip().startswith('#'):
                parts = line.strip().split()
                if parts:
                    job_name = parts[0].strip()
                    if job_name and job_name.lower() != 'job':
                        print(f"    - {job_name}")
                        if job_name not in [j.get("name") for j in all_jobs]:
                            all_jobs.append({
                                "name": job_name,
                                "source": "dsjob_dsadm"
                            })
    else:
        print("  ❌ dsjob 명령어 실행 실패")
        if result["stderr"]:
            print(f"    오류: {result['stderr']}")
        if result["stdout"]:
            print(f"    출력: {result['stdout']}")
    
    # 결과 요약
    print("\n" + "=" * 70)
    print("검색 결과 요약")
    print("=" * 70)
    
    if all_jobs:
        # 중복 제거
        unique_jobs = {}
        for job in all_jobs:
            job_name = job.get("name")
            if job_name and job_name not in unique_jobs:
                unique_jobs[job_name] = job
        
        print(f"\n✅ 총 {len(unique_jobs)}개 Job 발견:")
        for job_name, job_info in sorted(unique_jobs.items()):
            print(f"  - {job_name} (출처: {job_info.get('source', 'unknown')})")
            if job_info.get("path"):
                print(f"    경로: {job_info['path']}")
    else:
        print("\n❌ Job을 찾을 수 없음")
    
    client.close()
    print("\n" + "=" * 70)

if __name__ == "__main__":
    find_jobs_with_sudo()

