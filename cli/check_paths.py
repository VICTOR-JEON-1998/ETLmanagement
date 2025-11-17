"""SSH를 통한 DataStage 서버 경로 확인 스크립트"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.ssh_client import DataStageSSHClient
from src.core.logger import setup_logger

setup_logger("path_check", level="INFO")

def check_paths():
    """실제 파일 시스템 경로 확인"""
    client = DataStageSSHClient()
    
    print("=" * 70)
    print("DataStage 서버 파일 시스템 경로 확인")
    print("=" * 70)
    
    if not client.connect():
        print("\n❌ SSH 연결 실패")
        print("\n확인 사항:")
        print("1. config/config.yaml의 SSH 설정 확인")
        print("2. 비밀번호가 정확한지 확인 (기본값: Fila2023!)")
        print("3. 네트워크 연결 확인")
        return
    
    print("\n✅ SSH 연결 성공\n")
    
    # 1. 프로젝트 디렉토리 찾기
    print("=" * 70)
    print("1. BIDW_ADM 프로젝트 디렉토리 찾기")
    print("=" * 70)
    
    search_paths = [
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM",
        "/opt/IBM/InformationServer/Projects/BIDW_ADM",
        "/home/dsadm/Projects/BIDW_ADM",
        "/home/dsadm/BIDW_ADM",
    ]
    
    found_paths = []
    for path in search_paths:
        print(f"\n[일반 접근] {path}")
        result = client.execute_command(f"test -d '{path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        if "EXISTS" in result["stdout"]:
            print(f"  ✅ 존재함")
            found_paths.append((path, False))
            
            # 디렉토리 내용 확인
            list_result = client.execute_command(f"ls -1 '{path}' 2>/dev/null | head -10")
            if list_result["stdout"].strip():
                print(f"  내용 (일부):")
                for line in list_result["stdout"].strip().split('\n')[:5]:
                    if line.strip():
                        print(f"    - {line.strip()}")
        else:
            print(f"  ❌ 없음")
        
        # sudo -i로 시도
        print(f"\n[sudo -i 접근] {path}")
        result_sudo = client.execute_command(f"test -d '{path}' && echo 'EXISTS' || echo 'NOT_EXISTS'", use_sudo=True)
        if "EXISTS" in result_sudo["stdout"]:
            print(f"  ✅ 존재함 (sudo -i)")
            if (path, False) not in found_paths:
                found_paths.append((path, True))
            
            # 디렉토리 내용 확인
            list_result = client.execute_command(f"ls -1 '{path}' 2>/dev/null | head -10", use_sudo=True)
            if list_result["stdout"].strip():
                print(f"  내용 (일부):")
                for line in list_result["stdout"].strip().split('\n')[:5]:
                    if line.strip():
                        print(f"    - {line.strip()}")
        else:
            print(f"  ❌ 없음")
    
    # 전체 검색
    if not found_paths:
        print("\n[전체 검색] find 명령어로 BIDW_ADM 찾기")
        find_cmd = "find /opt /home -type d -name 'BIDW_ADM' 2>/dev/null | head -10"
        result = client.execute_command(find_cmd, use_sudo=True)
        if result["stdout"].strip():
            print("  찾은 경로:")
            for line in result["stdout"].strip().split('\n'):
                if line.strip():
                    print(f"    ✅ {line.strip()}")
                    found_paths.append((line.strip(), True))
        else:
            print("  ❌ 찾을 수 없음")
    
    # 2. Job 파일 찾기
    print("\n" + "=" * 70)
    print("2. Job 파일 (.dsx, .isx) 찾기")
    print("=" * 70)
    
    if found_paths:
        for project_path, use_sudo in found_paths:
            print(f"\n[검색 경로] {project_path}")
            for ext in ['*.dsx', '*.isx']:
                find_cmd = f"find '{project_path}' -name '{ext}' -type f 2>/dev/null | head -10"
                result = client.execute_command(find_cmd, use_sudo=use_sudo)
                if result["stdout"].strip():
                    print(f"  {ext} 파일:")
                    for line in result["stdout"].strip().split('\n')[:5]:
                        if line.strip():
                            print(f"    - {line.strip()}")
    else:
        print("\n프로젝트 경로를 찾지 못해 Job 파일 검색을 건너뜁니다.")
    
    # 3. dsjob 명령어 확인 및 Job 목록 조회
    print("\n" + "=" * 70)
    print("3. dsjob 명령어로 Job 목록 조회")
    print("=" * 70)
    
    # dsjob 경로 확인
    print("\n[dsjob 경로 확인]")
    dsjob_paths = [
        "which dsjob",
        "find /opt -name dsjob -type f 2>/dev/null | head -1",
    ]
    
    dsjob_found = False
    for cmd in dsjob_paths:
        result = client.execute_command(cmd, use_sudo=True)
        if result["stdout"].strip() and "not found" not in result["stdout"].lower():
            print(f"  ✅ {result['stdout'].strip()}")
            dsjob_found = True
            break
    
    if not dsjob_found:
        print("  ❌ dsjob 명령어를 찾을 수 없음")
    
    # DataStage 환경 설정 후 Job 목록 조회
    print("\n[Job 목록 조회]")
    dsenv_paths = [
        "/opt/IBM/InformationServer/Server/DSEngine/dsenv",
        "/opt/IBM/InformationServer/DSEngine/dsenv",
    ]
    
    for dsenv_path in dsenv_paths:
        # dsenv 파일 존재 확인
        check_result = client.execute_command(f"test -f '{dsenv_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'", use_sudo=True)
        if "EXISTS" in check_result["stdout"]:
            print(f"  dsenv 경로: {dsenv_path}")
            
            # 환경 설정 후 Job 목록 조회
            dsjob_cmd = f"source '{dsenv_path}' 2>/dev/null; dsjob -listjobs BIDW_ADM 2>&1"
            result = client.execute_command(dsjob_cmd, use_sudo=True)
            
            if result["exit_status"] == 0 and result["stdout"].strip():
                print(f"  ✅ Job 목록:")
                for line in result["stdout"].strip().split('\n')[:20]:
                    if line.strip() and not line.strip().startswith('#'):
                        print(f"    - {line.strip()}")
                break
            else:
                print(f"  ❌ Job 목록 조회 실패")
                if result["stderr"]:
                    print(f"    오류: {result['stderr']}")
    
    # 4. 프로젝트 디렉토리 구조 확인
    print("\n" + "=" * 70)
    print("4. 프로젝트 디렉토리 구조 확인")
    print("=" * 70)
    
    if found_paths:
        for project_path, use_sudo in found_paths[:1]:  # 첫 번째 경로만 상세 확인
            print(f"\n[경로] {project_path}")
            
            # 디렉토리 구조 확인
            tree_cmd = f"find '{project_path}' -maxdepth 3 -type d 2>/dev/null | head -30"
            result = client.execute_command(tree_cmd, use_sudo=use_sudo)
            if result["stdout"].strip():
                print("  디렉토리 구조:")
                for line in result["stdout"].strip().split('\n')[:20]:
                    if line.strip():
                        print(f"    {line.strip()}")
            
            # Job 관련 파일/디렉토리 찾기
            job_search_cmd = f"find '{project_path}' -type d -name '*JOB*' -o -name '*Job*' 2>/dev/null | head -20"
            result = client.execute_command(job_search_cmd, use_sudo=use_sudo)
            if result["stdout"].strip():
                print("\n  Job 관련 디렉토리:")
                for line in result["stdout"].strip().split('\n')[:10]:
                    if line.strip():
                        print(f"    - {line.strip()}")
    
    # 5. 현재 사용자 및 권한 확인
    print("\n" + "=" * 70)
    print("5. 사용자 및 권한 확인")
    print("=" * 70)
    
    whoami_result = client.execute_command("whoami")
    print(f"\n현재 사용자: {whoami_result['stdout'].strip()}")
    
    id_result = client.execute_command("id")
    print(f"사용자 ID: {id_result['stdout'].strip()}")
    
    # sudo 권한 확인
    sudo_check = client.execute_command("sudo -n true 2>&1", use_sudo=False)
    if sudo_check["exit_status"] == 0:
        print("✅ sudo 권한 있음")
    else:
        print("⚠️  sudo 권한 확인 필요 (비밀번호 입력 필요할 수 있음)")
    
    client.close()
    print("\n" + "=" * 70)
    print("경로 확인 완료")
    print("=" * 70)

if __name__ == "__main__":
    check_paths()

