"""SSH 연결 최종 테스트"""

import paramiko
import sys

def test_ssh():
    """SSH 연결 테스트"""
    host = "10.100.20.71"
    port = 22
    username = "useradmin"
    password = "Fila2023!"
    
    print(f"SSH 연결 테스트:")
    print(f"  Host: {host}")
    print(f"  Username: {username}")
    print(f"  Password: {'*' * len(password)}")
    
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=host,
            port=port,
            username=username,
            password=password,
            timeout=10
        )
        print("\nSSH 연결 성공!")
        
        # 현재 사용자 확인
        stdin, stdout, stderr = client.exec_command("whoami")
        user = stdout.read().decode('utf-8').strip()
        print(f"현재 사용자: {user}")
        
        # 프로젝트 경로 확인
        stdin, stdout, stderr = client.exec_command("test -d '/opt/IBM/InformationServer/Server/Projects/BIDW_ADM' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        result = stdout.read().decode('utf-8').strip()
        print(f"프로젝트 경로 존재: {result}")
        
        # Jobs 디렉토리 확인
        stdin, stdout, stderr = client.exec_command("test -d '/opt/IBM/InformationServer/Server/Projects/BIDW_ADM/Jobs' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        result = stdout.read().decode('utf-8').strip()
        print(f"Jobs 디렉토리 존재: {result}")
        
        # Jobs 디렉토리 내용 확인
        if result == "EXISTS":
            stdin, stdout, stderr = client.exec_command("ls -1 '/opt/IBM/InformationServer/Server/Projects/BIDW_ADM/Jobs' 2>/dev/null | head -30")
            jobs = stdout.read().decode('utf-8').strip()
            if jobs:
                print(f"\nJobs 디렉토리 내용 ({len(jobs.split())}개 항목):")
                for line in jobs.split('\n')[:20]:
                    if line.strip():
                        print(f"  - {line.strip()}")
            else:
                err = stderr.read().decode('utf-8').strip()
                if err:
                    print(f"오류: {err}")
        
        # dsjob 명령어 테스트
        print("\ndsjob 명령어 테스트:")
        stdin, stdout, stderr = client.exec_command("source /opt/IBM/InformationServer/Server/DSEngine/dsenv 2>/dev/null; dsjob -listjobs BIDW_ADM 2>&1 | head -30")
        dsjob_output = stdout.read().decode('utf-8').strip()
        dsjob_error = stderr.read().decode('utf-8').strip()
        
        if dsjob_output:
            print("dsjob 출력:")
            for line in dsjob_output.split('\n')[:20]:
                if line.strip():
                    print(f"  {line.strip()}")
        if dsjob_error:
            print(f"dsjob 오류: {dsjob_error}")
        
        client.close()
        return True
        
    except paramiko.AuthenticationException:
        print("\n인증 실패: 비밀번호가 잘못되었습니다.")
        return False
    except Exception as e:
        print(f"\n오류 발생: {type(e).__name__}: {e}")
        return False

if __name__ == "__main__":
    test_ssh()

