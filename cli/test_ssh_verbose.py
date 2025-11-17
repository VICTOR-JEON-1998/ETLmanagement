"""SSH 연결 상세 디버깅"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import paramiko
import logging

# Paramiko 로깅 활성화
logging.basicConfig(level=logging.DEBUG)
paramiko.util.log_to_file('ssh_debug.log')

def test_ssh_verbose():
    """SSH 연결 상세 테스트"""
    host = "10.100.20.70"
    port = 22
    username = "etl_admin"
    password = "etletl"
    
    print(f"SSH 연결 상세 테스트")
    print(f"호스트: {host}")
    print(f"포트: {port}")
    print(f"사용자: {username}")
    print(f"비밀번호: {repr(password)}")
    print(f"비밀번호 바이트: {password.encode('utf-8')}")
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
            allow_agent=False,
            compress=False
        )
        
        print("✓ SSH 연결 성공!")
        ssh.close()
        return True
        
    except paramiko.AuthenticationException as e:
        print(f"✗ 인증 실패: {e}")
        print(f"오류 타입: {type(e)}")
        return False
    except Exception as e:
        print(f"✗ 오류: {e}")
        print(f"오류 타입: {type(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_ssh_verbose()
