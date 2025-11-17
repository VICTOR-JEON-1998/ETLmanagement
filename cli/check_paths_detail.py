"""SSH를 통한 DataStage 서버 경로 상세 확인"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.ssh_client import DataStageSSHClient
from src.core.logger import setup_logger

setup_logger("path_check", level="INFO")

def check_paths_detail():
    """상세 경로 확인"""
    client = DataStageSSHClient()
    
    if not client.connect():
        print("SSH 연결 실패")
        return
    
    print("=" * 70)
    print("상세 경로 확인")
    print("=" * 70)
    
    # 확인된 프로젝트 경로들
    project_paths = [
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM",
        "/opt/IBM/InformationServer/Projects/BIDW_ADM",
        "/home/dsadm/Projects/BIDW_ADM",
        "/home/dsadm/BIDW_ADM",
    ]
    
    for project_path in project_paths:
        print(f"\n{'='*70}")
        print(f"경로: {project_path}")
        print(f"{'='*70}")
        
        # 디렉토리 존재 확인
        check_result = client.execute_command(f"test -d '{project_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        if "EXISTS" not in check_result["stdout"]:
            print("  ❌ 디렉토리가 존재하지 않음")
            continue
        
        # 디렉토리 내용 확인
        print("\n[디렉토리 내용]")
        list_result = client.execute_command(f"ls -la '{project_path}' 2>/dev/null | head -30")
        if list_result["stdout"].strip():
            print(list_result["stdout"].strip())
        else:
            print("  (내용 없음)")
        
        # 하위 디렉토리 확인
        print("\n[하위 디렉토리]")
        dirs_result = client.execute_command(f"find '{project_path}' -maxdepth 2 -type d 2>/dev/null | head -30")
        if dirs_result["stdout"].strip():
            for line in dirs_result["stdout"].strip().split('\n')[:20]:
                if line.strip():
                    print(f"  {line.strip()}")
        
        # 모든 파일 찾기
        print("\n[모든 파일 (일부)]")
        files_result = client.execute_command(f"find '{project_path}' -type f 2>/dev/null | head -20")
        if files_result["stdout"].strip():
            for line in files_result["stdout"].strip().split('\n')[:15]:
                if line.strip():
                    print(f"  {line.strip()}")
        
        # Job 관련 파일 찾기
        print("\n[Job 관련 파일]")
        job_files = client.execute_command(f"find '{project_path}' -type f \\( -name '*.dsx' -o -name '*.isx' -o -name '*JOB*' -o -name '*Job*' \\) 2>/dev/null | head -20")
        if job_files["stdout"].strip():
            for line in job_files["stdout"].strip().split('\n')[:15]:
                if line.strip():
                    print(f"  ✅ {line.strip()}")
        else:
            print("  (Job 파일을 찾을 수 없음)")
        
        # 디렉토리 크기 확인
        print("\n[디렉토리 크기]")
        size_result = client.execute_command(f"du -sh '{project_path}' 2>/dev/null")
        if size_result["stdout"].strip():
            print(f"  {size_result['stdout'].strip()}")
    
    # DataStage 설치 경로 확인
    print(f"\n{'='*70}")
    print("DataStage 설치 경로 확인")
    print(f"{'='*70}")
    
    ds_paths = [
        "/opt/IBM/InformationServer",
        "/opt/IBM/InformationServer/Server",
        "/opt/IBM/InformationServer/Server/DSEngine",
    ]
    
    for ds_path in ds_paths:
        check_result = client.execute_command(f"test -d '{ds_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        if "EXISTS" in check_result["stdout"]:
            print(f"\n✅ {ds_path}")
            # dsenv 파일 확인
            dsenv_result = client.execute_command(f"test -f '{ds_path}/dsenv' && echo 'EXISTS' || echo 'NOT_EXISTS'")
            if "EXISTS" in dsenv_result["stdout"]:
                print(f"  ✅ dsenv 파일 존재: {ds_path}/dsenv")
                
                # dsenv 소싱 후 dsjob 확인
                dsjob_check = client.execute_command(f"source '{ds_path}/dsenv' 2>/dev/null; which dsjob 2>&1")
                if dsjob_check["stdout"].strip() and "not found" not in dsjob_check["stdout"].lower():
                    print(f"  ✅ dsjob 경로: {dsjob_check['stdout'].strip()}")
                    
                    # Job 목록 조회 시도
                    job_list = client.execute_command(f"source '{ds_path}/dsenv' 2>/dev/null; dsjob -listjobs BIDW_ADM 2>&1")
                    if job_list["exit_status"] == 0 and job_list["stdout"].strip():
                        print(f"\n  ✅ Job 목록:")
                        for line in job_list["stdout"].strip().split('\n')[:30]:
                            if line.strip() and not line.strip().startswith('#'):
                                print(f"    - {line.strip()}")
                    else:
                        print(f"  ⚠️  Job 목록 조회 실패")
                        if job_list["stderr"]:
                            print(f"    오류: {job_list['stderr']}")
    
    client.close()

if __name__ == "__main__":
    check_paths_detail()

