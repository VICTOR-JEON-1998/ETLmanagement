"""SSH를 통한 DataStage 서버 경로 간단 확인"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.ssh_client import DataStageSSHClient

def check_simple():
    """간단한 경로 확인"""
    client = DataStageSSHClient()
    
    if not client.connect():
        print("SSH 연결 실패")
        return
    
    print("=" * 70)
    print("프로젝트 경로 상세 확인")
    print("=" * 70)
    
    # 확인된 경로들
    paths = [
        "/opt/IBM/InformationServer/Server/Projects/BIDW_ADM",
        "/home/dsadm/Projects/BIDW_ADM",
    ]
    
    for path in paths:
        print(f"\n{'='*70}")
        print(f"경로: {path}")
        print(f"{'='*70}")
        
        # ls -la로 내용 확인
        result = client.execute_command(f"ls -la '{path}' 2>/dev/null")
        if result["stdout"].strip():
            print("\n디렉토리 내용:")
            print(result["stdout"].strip()[:1000])  # 처음 1000자만
        
        # find로 하위 구조 확인
        result = client.execute_command(f"find '{path}' -maxdepth 3 -type d 2>/dev/null | head -20")
        if result["stdout"].strip():
            print("\n하위 디렉토리 구조:")
            for line in result["stdout"].strip().split('\n')[:15]:
                if line.strip():
                    print(f"  {line.strip()}")
        
        # 파일 찾기
        result = client.execute_command(f"find '{path}' -type f 2>/dev/null | head -20")
        if result["stdout"].strip():
            print("\n파일 목록:")
            for line in result["stdout"].strip().split('\n')[:15]:
                if line.strip():
                    print(f"  {line.strip()}")
    
    # DataStage dsenv 찾기
    print(f"\n{'='*70}")
    print("DataStage dsenv 파일 찾기")
    print(f"{'='*70}")
    
    result = client.execute_command("find /opt -name dsenv -type f 2>/dev/null | head -5")
    if result["stdout"].strip():
        print("찾은 dsenv 파일:")
        for line in result["stdout"].strip().split('\n'):
            if line.strip():
                print(f"  ✅ {line.strip()}")
                
                # dsenv 소싱 후 dsjob 확인
                dsjob_cmd = f"source '{line.strip()}' 2>/dev/null; dsjob -listjobs BIDW_ADM 2>&1 | head -30"
                job_result = client.execute_command(dsjob_cmd)
                if job_result["stdout"].strip():
                    print(f"\n  Job 목록:")
                    for job_line in job_result["stdout"].strip().split('\n')[:20]:
                        if job_line.strip() and not job_line.strip().startswith('#'):
                            print(f"    - {job_line.strip()}")
    else:
        print("  ❌ dsenv 파일을 찾을 수 없음")
    
    client.close()

if __name__ == "__main__":
    check_simple()

