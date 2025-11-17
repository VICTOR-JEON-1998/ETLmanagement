"""SSH 연결 디버깅"""

import paramiko
import sys

def test_ssh_debug():
    """SSH 연결 디버깅"""
    host = "10.100.20.70"
    port = 22
    username = "useradmin"
    password = "Fila2023!"
    
    print(f"SSH 연결 정보:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Username: {username}")
    print(f"  Password (repr): {repr(password)}")
    print(f"  Password (bytes): {password.encode('utf-8')}")
    print(f"  Password length: {len(password)}")
    
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        print("\n연결 시도...")
        client.connect(
            hostname=host,
            port=port,
            username=username,
            password=password,
            timeout=10,
            look_for_keys=False,
            allow_agent=False
        )
        print("SSH 연결 성공!")
        
        stdin, stdout, stderr = client.exec_command("whoami")
        user = stdout.read().decode('utf-8').strip()
        print(f"현재 사용자: {user}")
        
        client.close()
        return True
        
    except paramiko.AuthenticationException as e:
        print(f"\n인증 실패: {e}")
        print("\n가능한 원인:")
        print("1. 비밀번호가 정확하지 않음")
        print("2. 계정이 잠겨있음")
        print("3. 특수문자 인코딩 문제")
        return False
    except paramiko.SSHException as e:
        print(f"\nSSH 오류: {e}")
        return False
    except Exception as e:
        print(f"\n오류 발생: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_ssh_debug()

