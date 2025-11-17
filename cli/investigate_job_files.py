"""Job 파일을 찾지 못하는 이유 조사"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.ssh_client import DataStageSSHClient

def investigate():
    """Job 파일을 찾지 못하는 이유 조사"""
    client = DataStageSSHClient()
    
    if not client.connect():
        print("SSH 연결 실패")
        return
    
    print("=" * 70)
    print("Job 파일을 찾지 못하는 이유 조사")
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
        
        # 1. 디렉토리 존재 및 권한 확인
        print("\n[1. 디렉토리 존재 및 권한 확인]")
        check_result = client.execute_command(f"test -d '{project_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
        if "EXISTS" not in check_result["stdout"]:
            print("  ❌ 디렉토리가 존재하지 않음")
            continue
        
        # 권한 확인
        perm_result = client.execute_command(f"ls -ld '{project_path}' 2>/dev/null")
        if perm_result["stdout"].strip():
            print(f"  {perm_result['stdout'].strip()}")
        
        # 2. 디렉토리 내용 확인 (ls -la)
        print("\n[2. 디렉토리 내용 확인 (ls -la)]")
        list_result = client.execute_command(f"ls -la '{project_path}' 2>/dev/null")
        if list_result["stdout"].strip():
            lines = list_result["stdout"].strip().split('\n')
            print(f"  총 {len(lines)}개 항목")
            for line in lines[:20]:  # 처음 20개만
                if line.strip():
                    print(f"    {line.strip()}")
        else:
            print("  ❌ 내용을 읽을 수 없음 (권한 문제 가능)")
            # stderr 확인
            if list_result["stderr"]:
                print(f"    오류: {list_result['stderr']}")
        
        # 3. 모든 파일 확장자 확인
        print("\n[3. 모든 파일 확장자 확인]")
        ext_result = client.execute_command(f"find '{project_path}' -type f 2>/dev/null | head -50 | sed 's/.*\\.//' | sort | uniq -c | sort -rn")
        if ext_result["stdout"].strip():
            print("  파일 확장자 통계:")
            print(ext_result["stdout"].strip())
        else:
            print("  ❌ 파일을 찾을 수 없음")
            # 오류 확인
            if ext_result["stderr"]:
                print(f"    오류: {ext_result['stderr']}")
        
        # 4. .dsx, .isx 파일 직접 검색
        print("\n[4. .dsx, .isx 파일 직접 검색]")
        for ext in ['.dsx', '.isx', '.DSX', '.ISX']:
            search_cmd = f"find '{project_path}' -name '*{ext}' -type f 2>/dev/null | head -10"
            result = client.execute_command(search_cmd)
            if result["stdout"].strip():
                print(f"  ✅ {ext} 파일 발견:")
                for line in result["stdout"].strip().split('\n')[:5]:
                    if line.strip():
                        print(f"    - {line.strip()}")
            else:
                print(f"  ❌ {ext} 파일 없음")
                if result["stderr"]:
                    print(f"    오류: {result['stderr']}")
        
        # 5. Job 관련 모든 파일 검색 (대소문자 구분 없이)
        print("\n[5. Job 관련 모든 파일 검색]")
        job_search = client.execute_command(f"find '{project_path}' -type f -iname '*job*' 2>/dev/null | head -20")
        if job_search["stdout"].strip():
            print("  Job 관련 파일:")
            for line in job_search["stdout"].strip().split('\n')[:15]:
                if line.strip():
                    print(f"    - {line.strip()}")
        else:
            print("  ❌ Job 관련 파일 없음")
        
        # 6. 숨김 파일 포함 전체 검색
        print("\n[6. 숨김 파일 포함 전체 검색]")
        all_files = client.execute_command(f"find '{project_path}' -type f 2>/dev/null | head -30")
        if all_files["stdout"].strip():
            print("  모든 파일 (일부):")
            for line in all_files["stdout"].strip().split('\n')[:20]:
                if line.strip():
                    print(f"    {line.strip()}")
        else:
            print("  ❌ 파일을 찾을 수 없음")
            if all_files["stderr"]:
                print(f"    오류: {all_files['stderr']}")
        
        # 7. 디렉토리 구조 확인
        print("\n[7. 디렉토리 구조 확인]")
        dirs_result = client.execute_command(f"find '{project_path}' -type d 2>/dev/null | head -30")
        if dirs_result["stdout"].strip():
            print("  디렉토리 구조:")
            for line in dirs_result["stdout"].strip().split('\n')[:20]:
                if line.strip():
                    print(f"    {line.strip()}")
        else:
            print("  ❌ 디렉토리를 찾을 수 없음")
        
        # 8. DataStage Repository 구조 확인
        print("\n[8. DataStage Repository 구조 확인]")
        # DataStage는 보통 Repository를 사용하므로 특정 디렉토리 구조를 가질 수 있음
        repo_dirs = ['Jobs', 'Routines', 'Table Definitions', 'Shared Containers']
        for repo_dir in repo_dirs:
            repo_path = f"{project_path}/{repo_dir}"
            check_repo = client.execute_command(f"test -d '{repo_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
            if "EXISTS" in check_repo["stdout"]:
                print(f"  ✅ {repo_dir} 디렉토리 존재")
                # 내용 확인
                repo_content = client.execute_command(f"ls -1 '{repo_path}' 2>/dev/null | head -10")
                if repo_content["stdout"].strip():
                    print(f"    내용:")
                    for line in repo_content["stdout"].strip().split('\n')[:5]:
                        if line.strip():
                            print(f"      - {line.strip()}")
            else:
                print(f"  ❌ {repo_dir} 디렉토리 없음")
        
        # 한 경로만 상세 확인
        break
    
    # 9. DataStage Repository 데이터베이스 확인
    print(f"\n{'='*70}")
    print("DataStage Repository 데이터베이스 확인")
    print(f"{'='*70}")
    
    # DataStage는 보통 데이터베이스에 Job 정보를 저장함
    print("\n[DataStage Repository는 데이터베이스에 저장됨]")
    print("  DataStage Job은 파일 시스템이 아닌 Repository 데이터베이스에 저장됩니다.")
    print("  따라서 .dsx 파일은 Export할 때만 생성됩니다.")
    print("  Job 목록은 dsjob 명령어나 DataStage API를 통해 조회해야 합니다.")
    
    client.close()

if __name__ == "__main__":
    investigate()

