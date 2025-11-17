"""간단한 SSH 연결 테스트"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.ssh_client import DataStageSSHClient

def test_ssh():
    """SSH 연결 테스트"""
    client = DataStageSSHClient()
    
    print(f"SSH 연결 정보:")
    print(f"  Host: {client.ssh_host}")
    print(f"  Port: {client.ssh_port}")
    print(f"  Username: {client.ssh_username}")
    print(f"  Password length: {len(client.ssh_password) if client.ssh_password else 0}")
    print(f"  Password starts with: {client.ssh_password[:3] if client.ssh_password else 'None'}")
    
    print("\n연결 시도...")
    if client.connect():
        print("SSH 연결 성공!")
        
        # 간단한 명령어 실행
        result = client.execute_command("whoami")
        print(f"현재 사용자: {result['stdout'].strip()}")
        
        # 프로젝트 경로 확인
        project_path = "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM"
        check = client.execute_command(f"test -d '{project_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        print(f"프로젝트 경로 확인: {check['stdout'].strip()}")
        
        # Jobs 디렉토리 확인
        jobs_path = f"{project_path}/Jobs"
        check_jobs = client.execute_command(f"test -d '{jobs_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        print(f"Jobs 디렉토리 확인: {check_jobs['stdout'].strip()}")
        
        # Jobs 디렉토리 내용 확인
        if "EXISTS" in check_jobs['stdout']:
            list_jobs = client.execute_command(f"ls -1 '{jobs_path}' 2>/dev/null | head -20")
            if list_jobs['stdout'].strip():
                print(f"\nJobs 디렉토리 내용:")
                for line in list_jobs['stdout'].strip().split('\n')[:10]:
                    if line.strip():
                        print(f"  - {line.strip()}")
        
        client.close()
    else:
        print("SSH 연결 실패")

if __name__ == "__main__":
    test_ssh()

