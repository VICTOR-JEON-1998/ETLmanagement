"""SSH 직접 연결 테스트"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import paramiko
from src.core.config import get_config

def test_ssh():
    """SSH 연결 직접 테스트"""
    config = get_config().get_datastage_config()
    ssh_config = config.get("ssh", {})
    
    host = ssh_config.get("host", "10.100.20.70")
    port = ssh_config.get("port", 22)
    username = ssh_config.get("username", "etl_admin")
    password = ssh_config.get("password", "etletl")
    
    print(f"SSH 연결 테스트")
    print(f"호스트: {host}")
    print(f"포트: {port}")
    print(f"사용자: {username}")
    print(f"비밀번호 길이: {len(password)}")
    print(f"비밀번호: {password}")
    print()
    
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        print("연결 시도 중...")
        ssh.connect(
            hostname=host,
            port=port,
            username=username,
            password=password,
            timeout=10,
            look_for_keys=False,
            allow_agent=False
        )
        
        print("✓ SSH 연결 성공!")
        
        # 간단한 명령 실행
        stdin, stdout, stderr = ssh.exec_command('pwd')
        current_dir = stdout.read().decode().strip()
        print(f"현재 경로: {current_dir}")
        
        # DataStage 경로 확인
        stdin, stdout, stderr = ssh.exec_command('ls -la /opt/IBM/InformationServer/Server/Projects/ 2>/dev/null | head -5')
        projects = stdout.read().decode().strip()
        if projects:
            print(f"\n프로젝트 디렉토리:")
            print(projects)
        else:
            print("\n프로젝트 디렉토리를 찾을 수 없습니다.")
        
        ssh.close()
        return True
        
    except paramiko.AuthenticationException as e:
        print(f"✗ 인증 실패: {e}")
        print("\n가능한 원인:")
        print("  1. 비밀번호가 잘못되었습니다")
        print("  2. 사용자 이름이 잘못되었습니다")
        print("  3. 계정이 비활성화되었습니다")
        return False
    except paramiko.SSHException as e:
        print(f"✗ SSH 오류: {e}")
        return False
    except Exception as e:
        print(f"✗ 연결 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_ssh()
