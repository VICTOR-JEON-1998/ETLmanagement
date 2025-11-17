"""로컬 DSX 파일에서 Job 목록 확인"""

from src.datastage.local_client import DataStageLocalClient
from src.core.config import get_config

def main():
    print("=" * 60)
    print("로컬 DSX 파일에서 Job 목록 확인")
    print("=" * 60)
    
    config = get_config().get_datastage_config()
    export_path = config.get("local_export_path", "")
    
    print(f"\nExport 파일 경로: {export_path}")
    
    client = DataStageLocalClient()
    
    # 모든 프로젝트의 Job 조회
    print("\n[1] 모든 프로젝트의 Job 조회...")
    all_jobs = client.get_jobs(None)
    
    if all_jobs:
        print(f"✓ 총 {len(all_jobs)}개 Job 발견")
        
        # 프로젝트별로 그룹화
        projects = {}
        for job in all_jobs:
            project = job.get("project", "Unknown")
            if project not in projects:
                projects[project] = []
            projects[project].append(job)
        
        print(f"\n프로젝트별 Job 목록:")
        for project, jobs in projects.items():
            print(f"\n  [{project}] - {len(jobs)}개 Job:")
            for job in jobs:
                print(f"    - {job.get('name', 'Unknown')}")
    else:
        print("⚠ Job을 찾을 수 없습니다.")
        print("\n💡 Export 파일이 있는지 확인하세요:")
        print(f"   경로: {export_path}")
        print("\n💡 Export 방법:")
        print("   1. DataStage Designer 실행")
        print("   2. File → Export → DataStage Components...")
        print("   3. Jobs 선택 후 Export")
        print(f"   4. 파일을 다음 경로에 저장: {export_path}")
    
    # 특정 프로젝트 조회
    project_name = "BIDW_ADM"
    print(f"\n[2] 프로젝트 '{project_name}'의 Job 조회...")
    project_jobs = client.get_jobs(project_name)
    
    if project_jobs:
        print(f"✓ {len(project_jobs)}개 Job 발견:")
        for job in project_jobs:
            print(f"  - {job.get('name', 'Unknown')}")
    else:
        print(f"⚠ 프로젝트 '{project_name}'에서 Job을 찾을 수 없습니다.")

if __name__ == "__main__":
    main()

