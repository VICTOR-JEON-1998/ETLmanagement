"""다른 방법으로 Jobs 찾기"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.ssh_client import DataStageSSHClient

def find_jobs_alternative():
    """다른 방법으로 Jobs 찾기"""
    client = DataStageSSHClient()
    
    if not client.connect():
        print("SSH 연결 실패")
        return
    
    print("=" * 70)
    print("대안 방법으로 Jobs 찾기")
    print("=" * 70)
    
    # 방법 1: useradmin이 접근 가능한 경로 확인
    print("\n[방법 1] useradmin이 접근 가능한 경로 확인")
    
    # 현재 사용자 확인
    whoami = client.execute_command("whoami")
    print(f"현재 사용자: {whoami['stdout'].strip()}")
    
    # 홈 디렉토리 확인
    home = client.execute_command("echo $HOME")
    print(f"홈 디렉토리: {home['stdout'].strip()}")
    
    # 접근 가능한 DataStage 관련 경로 찾기
    accessible_paths = []
    test_paths = [
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM",
        "/home/dsadm/Projects/BIDW_ADM",
        "/tmp",
        home['stdout'].strip(),
    ]
    
    for path in test_paths:
        test_cmd = f"test -r '{path}' && echo 'READABLE' || echo 'NOT_READABLE'"
        result = client.execute_command(test_cmd)
        if "READABLE" in result["stdout"]:
            accessible_paths.append(path)
            print(f"  ✅ {path} - 읽기 가능")
        else:
            print(f"  ❌ {path} - 읽기 불가")
    
    # 방법 2: DataStage 클라이언트에서 확인한 Job 목록 기반 확인
    print("\n[방법 2] DataStage 클라이언트에서 확인한 Job 목록 기반 확인")
    print("확인된 Job 목록:")
    known_jobs = [
        "01,JOB",
        "02,JOB_NEW", 
        "03,JOB_EIS",
        "70,SEQ",
        "80,PARAM",
        "90,VERI",
        "99,SAMPLE"
    ]
    
    for job_name in known_jobs:
        print(f"  - {job_name}")
    
    # 방법 3: DataStage Repository 데이터베이스 확인
    print("\n[방법 3] DataStage Repository 정보 확인")
    
    # DataStage 설정 파일 확인
    config_paths = [
        "/opt/IBM/InformationServer/Server/Configurations",
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM",
    ]
    
    for config_path in config_paths:
        print(f"\n  경로: {config_path}")
        # 설정 파일 찾기
        config_files = client.execute_command(f"find '{config_path}' -name '*.cfg' -o -name '*.conf' -o -name '*.ini' 2>/dev/null | head -10")
        if config_files["stdout"].strip():
            print("  설정 파일:")
            for line in config_files["stdout"].strip().split('\n')[:5]:
                if line.strip():
                    print(f"    - {line.strip()}")
    
    # 방법 4: DataStage API 엔드포인트 확인
    print("\n[방법 4] DataStage API 사용 가능 여부 확인")
    print("  DataStage REST API를 통해 Job 목록 조회 시도")
    print("  (이미 구현된 api_client.py 사용)")
    
    # 방법 5: Export된 .dsx 파일 위치 확인
    print("\n[방법 5] Export된 .dsx 파일 위치 확인")
    print("  DataStage 클라이언트에서 Export한 파일 위치:")
    export_paths = [
        "/tmp",
        "/home/useradmin",
        "/home/dsadm",
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM",
    ]
    
    for export_path in export_paths:
        dsx_files = client.execute_command(f"find '{export_path}' -name '*.dsx' -type f 2>/dev/null | head -10")
        if dsx_files["stdout"].strip():
            print(f"  ✅ {export_path}에서 .dsx 파일 발견:")
            for line in dsx_files["stdout"].strip().split('\n')[:5]:
                if line.strip():
                    print(f"    - {line.strip()}")
    
    # 결론 및 권장 사항
    print("\n" + "=" * 70)
    print("결론 및 권장 사항")
    print("=" * 70)
    
    print("""
현재 상황:
1. Jobs 디렉토리는 존재하지만 useradmin 계정으로는 접근 불가
2. dsjob 명령어는 dsadm 계정 권한이 필요
3. .dsx 파일은 Export할 때만 생성됨 (파일 시스템에 영구 저장되지 않음)

해결 방안:
1. DataStage REST API 사용 (가장 권장)
   - DataStage 11.7+ 버전에서 지원
   - 인증 후 Job 목록 조회 가능

2. dsadm 계정으로 SSH 접속
   - config/config.yaml에서 SSH username을 'dsadm'으로 변경
   - dsjob 명령어 직접 사용 가능

3. DataStage 클라이언트에서 Export한 .dsx 파일 사용
   - Export 후 파일 위치 확인
   - 파일을 읽어서 Job 정보 추출

4. 확인된 Job 목록을 하드코딩 (임시 방법)
   - DataStage 클라이언트에서 확인한 Job 목록 사용
   - 프로그램에서 이 목록을 기본값으로 사용
""")
    
    client.close()

if __name__ == "__main__":
    find_jobs_alternative()

