"""DataStage 경로 디버깅 스크립트"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.ssh_client import DataStageSSHClient

def debug_datastage_paths():
    """DataStage 경로 디버깅"""
    client = DataStageSSHClient()
    
    if not client.connect():
        print("SSH 연결 실패")
        return
    
    print("=" * 60)
    print("DataStage 경로 디버깅")
    print("=" * 60)
    
    # 1. DataStage 설치 경로 찾기
    print("\n1. DataStage 설치 경로 찾기:")
    ds_path = client.find_datastage_path()
    print(f"   DataStage 경로: {ds_path}")
    
    # 2. 프로젝트 디렉토리 찾기
    print("\n2. 프로젝트 디렉토리 찾기:")
    search_paths = [
        "/opt/IBM/InformationServer/Server/Projects",
        "/opt/IBM/InformationServer/Projects",
        "/home/dsadm/Projects",
        "/home/dsadm",
        "/opt/IBM",
    ]
    
    for path in search_paths:
        # 일반 접근 시도
        result = client.execute_command(f"test -d '{path}' && echo 'exists' || echo 'not exists'")
        exists = "exists" in result["stdout"]
        print(f"   {path}: {'✓ 존재' if exists else '✗ 없음'} (일반)")
        
        # sudo -i로 root 전환 후 접근 시도
        if not exists:
            result = client.execute_command(f"test -d '{path}' && echo 'exists' || echo 'not exists'", use_sudo=True)
            exists = "exists" in result["stdout"]
            print(f"   {path}: {'✓ 존재' if exists else '✗ 없음'} (sudo -i)")
        
        if exists:
            # 디렉토리 내용 확인
            list_result = client.execute_command(f"ls -1 '{path}' 2>/dev/null | head -10")
            if not list_result["stdout"].strip():
                list_result = client.execute_command(f"ls -1 '{path}' 2>/dev/null | head -10", use_sudo=True)
            if list_result["exit_status"] == 0 and list_result["stdout"].strip():
                print(f"     내용: {list_result['stdout'].strip()[:200]}")
    
    # 3. BIDW_ADM 프로젝트 찾기
    print("\n3. BIDW_ADM 프로젝트 찾기:")
    find_cmd = "find /opt /home -type d -name 'BIDW_ADM' 2>/dev/null | head -5"
    result = client.execute_command(find_cmd)
    if result["exit_status"] == 0 and result["stdout"].strip():
        print("   찾은 경로 (일반):")
        for line in result["stdout"].strip().split('\n'):
            if line.strip():
                print(f"     - {line.strip()}")
    else:
        print("   일반 접근으로 찾을 수 없음, sudo -i로 시도...")
        result = client.execute_command(find_cmd, use_sudo=True)
        if result["exit_status"] == 0 and result["stdout"].strip():
            print("   찾은 경로 (sudo -i):")
            for line in result["stdout"].strip().split('\n'):
                if line.strip():
                    print(f"     - {line.strip()}")
        else:
            print("   BIDW_ADM 프로젝트를 찾을 수 없습니다")
    
    # 4. dsjob 명령어 확인
    print("\n4. dsjob 명령어 확인:")
    dsjob_check = "which dsjob 2>/dev/null || echo 'not found'"
    result = client.execute_command(dsjob_check)
    print(f"   dsjob 경로: {result['stdout'].strip()}")
    
    # 5. DataStage 환경 변수 확인
    print("\n5. DataStage 환경 변수 확인:")
    env_check = "env | grep -i datastage | head -5 || echo 'not found'"
    result = client.execute_command(env_check)
    if result["stdout"].strip():
        print(f"   {result['stdout'].strip()}")
    else:
        print("   DataStage 환경 변수를 찾을 수 없습니다")
    
    # 6. 프로젝트 목록 조회
    print("\n6. 프로젝트 목록:")
    projects = client.get_datastage_projects()
    print(f"   찾은 프로젝트: {projects}")
    
    # 7. BIDW_ADM 프로젝트 내용 확인
    print("\n7. BIDW_ADM 프로젝트 내용 확인:")
    # 직접 경로 확인
    test_paths = [
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM",
        "/home/dsadm/Projects/BIDW_ADM",
        "/opt/IBM/InformationServer/Projects/BIDW_ADM",
    ]
    
    found_path = None
    for path in test_paths:
        result = client.execute_command(f"test -d '{path}' && echo 'exists' || echo 'not exists'", use_sudo=True)
        if "exists" in result["stdout"]:
            found_path = path
            print(f"   프로젝트 경로 발견: {found_path}")
            
            # 디렉토리 내용 확인
            list_result = client.execute_command(f"ls -1 '{found_path}' 2>/dev/null | head -20", use_sudo=True)
            if list_result["exit_status"] == 0 and list_result["stdout"].strip():
                print(f"   프로젝트 내용 ({len(list_result['stdout'].strip().split())}개 항목):")
                for line in list_result["stdout"].strip().split('\n')[:10]:
                    if line.strip():
                        print(f"     - {line.strip()}")
            break
    
    if not found_path:
        print("   BIDW_ADM 프로젝트 경로를 찾을 수 없습니다")
    
    # Job 목록 조회
    print("\n8. Job 목록 조회:")
    jobs = client.get_jobs("BIDW_ADM")
    print(f"   총 Job 수: {len(jobs)}")
    if jobs:
        print("   Job 목록 (최대 10개):")
        for job in jobs[:10]:
            print(f"     - {job.get('name')} ({job.get('source', 'unknown')})")
    
    client.close()
    print("\n" + "=" * 60)

if __name__ == "__main__":
    debug_datastage_paths()

