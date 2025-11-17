"""Job 파일과 경로를 종합적으로 찾기"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.ssh_client import DataStageSSHClient
from src.core.logger import setup_logger

setup_logger("find_jobs", level="INFO")

def find_jobs_comprehensive():
    """Job 파일과 경로를 종합적으로 찾기"""
    client = DataStageSSHClient()
    
    print("=" * 70)
    print("Job 파일 및 경로 종합 검색")
    print("=" * 70)
    
    # SSH 연결 재시도 (최대 3번)
    max_retries = 3
    connected = False
    for i in range(max_retries):
        print(f"\nSSH 연결 시도 {i+1}/{max_retries}...")
        if client.connect():
            connected = True
            print("✅ SSH 연결 성공\n")
            break
        else:
            if i < max_retries - 1:
                print("재시도 중...")
                import time
                time.sleep(2)
    
    if not connected:
        print("\n❌ SSH 연결 실패 - 모든 시도 실패")
        print("\n확인 사항:")
        print("1. config/config.yaml의 SSH 설정 확인")
        print("2. 비밀번호 확인 (기본값: Fila2023!)")
        print("3. 네트워크 연결 확인")
        return
    
    all_jobs = []
    
    # 방법 1: dsjob 명령어로 Job 목록 조회 (가장 확실한 방법)
    print("=" * 70)
    print("방법 1: dsjob 명령어로 Job 목록 조회")
    print("=" * 70)
    
    # dsenv 파일 찾기
    dsenv_paths = [
        "/opt/IBM/InformationServer/Server/DSEngine/dsenv",
        "/opt/IBM/InformationServer/DSEngine/dsenv",
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM/../DSEngine/dsenv",
    ]
    
    dsjob_found = False
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
            print(f"  ✅ dsjob 경로: {which_dsjob['stdout'].strip()}")
            dsjob_found = True
            
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
    
    if not dsjob_found:
        print("\n⚠️  dsjob 명령어를 찾을 수 없음")
    
    # 방법 2: 프로젝트 디렉토리에서 직접 찾기
    print("\n" + "=" * 70)
    print("방법 2: 프로젝트 디렉토리에서 직접 찾기")
    print("=" * 70)
    
    project_paths = [
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM",
        "/opt/IBM/InformationServer/Projects/BIDW_ADM",
        "/home/dsadm/Projects/BIDW_ADM",
        "/home/dsadm/BIDW_ADM",
    ]
    
    for project_path in project_paths:
        print(f"\n[경로] {project_path}")
        
        # 디렉토리 존재 확인
        check_result = client.execute_command(f"test -d '{project_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        if "EXISTS" not in check_result["stdout"]:
            print("  ❌ 디렉토리 없음")
            continue
        
        print("  ✅ 디렉토리 존재")
        
        # 디렉토리 내용 확인
        print("\n  [디렉토리 내용]")
        list_result = client.execute_command(f"ls -la '{project_path}' 2>/dev/null | head -50")
        if list_result["stdout"].strip():
            lines = list_result["stdout"].strip().split('\n')
            print(f"  총 {len(lines)}개 항목 발견")
            for line in lines[:30]:
                if line.strip() and not line.strip().startswith('total'):
                    print(f"    {line.strip()}")
        else:
            print("  ⚠️  내용을 읽을 수 없음 (권한 문제 가능)")
            if list_result["stderr"]:
                print(f"    오류: {list_result['stderr']}")
        
        # 하위 디렉토리 확인
        print("\n  [하위 디렉토리]")
        dirs_result = client.execute_command(f"find '{project_path}' -maxdepth 2 -type d 2>/dev/null | head -30")
        if dirs_result["stdout"].strip():
            dirs = dirs_result["stdout"].strip().split('\n')
            print(f"  총 {len(dirs)}개 디렉토리")
            for dir_path in dirs[:20]:
                if dir_path.strip():
                    print(f"    {dir_path.strip()}")
        
        # Job 관련 파일 검색
        print("\n  [Job 관련 파일 검색]")
        for pattern in ['*.dsx', '*.isx', '*JOB*', '*Job*', '*job*']:
            search_cmd = f"find '{project_path}' -type f -iname '{pattern}' 2>/dev/null | head -20"
            result = client.execute_command(search_cmd)
            if result["stdout"].strip():
                print(f"    ✅ {pattern} 파일 발견:")
                for line in result["stdout"].strip().split('\n')[:10]:
                    if line.strip():
                        print(f"      - {line.strip()}")
                        # Job 이름 추출
                        job_name = Path(line.strip()).stem
                        if job_name not in [j.get("name") for j in all_jobs]:
                            all_jobs.append({
                                "name": job_name,
                                "file_path": line.strip(),
                                "source": "file_system",
                                "method": f"find_{pattern}"
                            })
        
        # DataStage Repository 구조 확인
        print("\n  [Repository 구조 확인]")
        repo_items = ['Jobs', 'Routines', 'Table Definitions']
        for item in repo_items:
            repo_path = f"{project_path}/{item}"
            check_repo = client.execute_command(f"test -d '{repo_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
            if "EXISTS" in check_repo["stdout"]:
                print(f"    ✅ {item} 디렉토리 존재")
                # 내용 확인
                repo_content = client.execute_command(f"ls -1 '{repo_path}' 2>/dev/null | head -20")
                if repo_content["stdout"].strip():
                    print(f"      내용:")
                    for line in repo_content["stdout"].strip().split('\n')[:10]:
                        if line.strip():
                            print(f"        - {line.strip()}")
                            # Job 이름으로 보이는 항목 추가
                            if item == 'Jobs' and line.strip() not in [j.get("name") for j in all_jobs]:
                                all_jobs.append({
                                    "name": line.strip(),
                                    "source": "repository",
                                    "method": f"repo_{item}"
                                })
    
    # 방법 3: 전체 시스템에서 Job 파일 검색
    print("\n" + "=" * 70)
    print("방법 3: 전체 시스템에서 Job 파일 검색")
    print("=" * 70)
    
    search_areas = [
        "/opt/IBM/InformationServer",
        "/home/dsadm",
    ]
    
    for area in search_areas:
        print(f"\n[검색 영역] {area}")
        for ext in ['.dsx', '.isx']:
            search_cmd = f"find '{area}' -name '*{ext}' -type f 2>/dev/null | grep -i BIDW_ADM | head -20"
            result = client.execute_command(search_cmd)
            if result["stdout"].strip():
                print(f"  ✅ {ext} 파일 발견:")
                for line in result["stdout"].strip().split('\n')[:10]:
                    if line.strip():
                        print(f"    - {line.strip()}")
    
    # 결과 요약
    print("\n" + "=" * 70)
    print("검색 결과 요약")
    print("=" * 70)
    
    if all_jobs:
        print(f"\n✅ 총 {len(all_jobs)}개 Job 발견:")
        # 중복 제거
        unique_jobs = {}
        for job in all_jobs:
            job_name = job.get("name")
            if job_name and job_name not in unique_jobs:
                unique_jobs[job_name] = job
        
        for job_name, job_info in unique_jobs.items():
            print(f"  - {job_name} (출처: {job_info.get('source', 'unknown')}, 방법: {job_info.get('method', 'unknown')})")
        
        print(f"\n총 {len(unique_jobs)}개 고유 Job")
    else:
        print("\n❌ Job을 찾을 수 없음")
        print("\n가능한 이유:")
        print("1. DataStage Repository에만 저장되어 있고 파일 시스템에 없음")
        print("2. 권한 문제로 접근 불가")
        print("3. 다른 경로에 저장됨")
        print("\n권장 사항:")
        print("- dsjob 명령어를 사용하여 Repository에서 직접 조회")
        print("- DataStage 클라이언트에서 Export한 .dsx 파일 위치 확인")
    
    client.close()
    print("\n" + "=" * 70)

if __name__ == "__main__":
    find_jobs_comprehensive()

