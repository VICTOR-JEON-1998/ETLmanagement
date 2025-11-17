"""dsadm 계정으로 Job 목록 조회"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.ssh_client import DataStageSSHClient
from src.core.logger import setup_logger

setup_logger("find_jobs", level="INFO")

def find_jobs_dsadm():
    """dsadm 계정으로 Job 목록 조회"""
    client = DataStageSSHClient()
    
    print("=" * 70)
    print("dsadm 계정으로 Job 목록 조회")
    print("=" * 70)
    
    if not client.connect():
        print("\n❌ SSH 연결 실패")
        print(f"연결 정보: {client.ssh_username}@{client.ssh_host}:{client.ssh_port}")
        return
    
    print(f"\n✅ SSH 연결 성공: {client.ssh_username}@{client.ssh_host}")
    
    # 현재 사용자 확인
    whoami = client.execute_command("whoami")
    print(f"현재 사용자: {whoami['stdout'].strip()}\n")
    
    all_jobs = []
    
    # 방법 1: dsjob 명령어로 Job 목록 조회
    print("=" * 70)
    print("방법 1: dsjob 명령어로 Job 목록 조회")
    print("=" * 70)
    
    dsenv_paths = [
        "/opt/IBM/InformationServer/Server/DSEngine/dsenv",
        "/opt/IBM/InformationServer/DSEngine/dsenv",
    ]
    
    for dsenv_path in dsenv_paths:
        print(f"\n[시도] {dsenv_path}")
        
        # dsenv 파일 존재 확인
        check_result = client.execute_command(f"test -f '{dsenv_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        if "EXISTS" not in check_result["stdout"]:
            print("  ❌ dsenv 파일 없음")
            continue
        
        print("  ✅ dsenv 파일 존재")
        
        # dsjob 경로 확인
        which_dsjob = client.execute_command(f"source '{dsenv_path}' 2>/dev/null; which dsjob 2>&1")
        if which_dsjob["stdout"].strip() and "not found" not in which_dsjob["stdout"].lower():
            dsjob_path = which_dsjob["stdout"].strip()
            print(f"  ✅ dsjob 경로: {dsjob_path}")
        else:
            print("  ⚠️  dsjob 경로를 찾을 수 없음 (환경 변수 설정 후 재시도)")
        
        # Job 목록 조회
        print(f"\n  [Job 목록 조회]")
        job_list_cmd = f"source '{dsenv_path}' 2>/dev/null; dsjob -listjobs BIDW_ADM 2>&1"
        job_result = client.execute_command(job_list_cmd)
        
        if job_result["exit_status"] == 0 and job_result["stdout"].strip():
            print("  ✅ Job 목록 조회 성공:")
            jobs_from_dsjob = []
            for line in job_result["stdout"].strip().split('\n'):
                if line.strip() and not line.strip().startswith('#'):
                    parts = line.strip().split()
                    if parts:
                        job_name = parts[0].strip()
                        if job_name and job_name.lower() != 'job':
                            jobs_from_dsjob.append(job_name)
                            print(f"    - {job_name}")
                            all_jobs.append({
                                "name": job_name,
                                "source": "dsjob",
                                "method": "dsjob_command"
                            })
            
            if jobs_from_dsjob:
                print(f"\n  총 {len(jobs_from_dsjob)}개 Job 발견 (dsjob)")
                break
        else:
            print("  ❌ Job 목록 조회 실패")
            if job_result["stderr"]:
                print(f"    오류: {job_result['stderr']}")
            if job_result["stdout"]:
                print(f"    출력: {job_result['stdout']}")
    
    # 방법 2: Jobs 디렉토리에서 직접 찾기
    print("\n" + "=" * 70)
    print("방법 2: Jobs 디렉토리에서 직접 찾기")
    print("=" * 70)
    
    project_paths = [
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM",
        "/home/dsadm/Projects/BIDW_ADM",
    ]
    
    for project_path in project_paths:
        jobs_path = f"{project_path}/Jobs"
        
        print(f"\n[경로] {jobs_path}")
        
        # Jobs 디렉토리 존재 확인
        check_result = client.execute_command(f"test -d '{jobs_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        if "EXISTS" not in check_result["stdout"]:
            print("  ❌ Jobs 디렉토리 없음")
            continue
        
        print("  ✅ Jobs 디렉토리 존재")
        
        # 디렉토리 내용 확인
        print("\n  [디렉토리 내용]")
        list_result = client.execute_command(f"ls -la '{jobs_path}' 2>/dev/null | head -100")
        if list_result["stdout"].strip():
            lines = list_result["stdout"].strip().split('\n')
            print(f"  총 {len(lines)}개 항목")
            
            for line in lines[:50]:
                if line.strip() and not line.strip().startswith('total'):
                    print(f"    {line.strip()}")
                    # 디렉토리 이름 추출
                    parts = line.strip().split()
                    if len(parts) >= 9:
                        item_name = parts[-1]
                        if item_name not in ['.', '..'] and item_name not in [j.get("name") for j in all_jobs]:
                            # 디렉토리인지 확인
                            if line.strip().startswith('d'):
                                all_jobs.append({
                                    "name": item_name,
                                    "path": f"{jobs_path}/{item_name}",
                                    "source": "jobs_directory"
                                })
        else:
            print("  ⚠️  내용을 읽을 수 없음")
            if list_result["stderr"]:
                print(f"    오류: {list_result['stderr']}")
        
        # 하위 디렉토리 확인
        print("\n  [하위 디렉토리]")
        find_cmd = f"find '{jobs_path}' -maxdepth 2 -type d 2>/dev/null | head -50"
        result = client.execute_command(find_cmd)
        
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
        
        print(f"\n총 {len(unique_jobs)}개 고유 Job")
    else:
        print("\n❌ Job을 찾을 수 없음")
    
    client.close()
    print("\n" + "=" * 70)

if __name__ == "__main__":
    find_jobs_dsadm()

