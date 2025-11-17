"""DataStage 연결 테스트 스크립트"""

from src.datastage import get_datastage_client
from src.core.logger import get_logger

logger = get_logger(__name__)


def main():
    """DataStage 연결 테스트"""
    print("=" * 60)
    print("IBM DataStage 연결 테스트")
    print("=" * 60)
    
    # 클라이언트 생성
    client = get_datastage_client()
    
    # 연결 테스트
    print("\n[1] 연결 테스트 중...")
    result = client.test_connection()
    
    if result["success"]:
        print(f"✓ 연결 성공!")
        print(f"  - 방법: {result.get('method', 'unknown')}")
        if result.get('status_code'):
            print(f"  - 상태 코드: {result.get('status_code')}")
        if result.get('endpoint'):
            print(f"  - 엔드포인트: {result.get('endpoint')}")
        
        # 프로젝트 목록 조회
        print("\n[2] 프로젝트 목록 조회 중...")
        projects = client.get_projects()
        if projects:
            print(f"✓ 프로젝트 {len(projects)}개 발견:")
            for project in projects[:10]:
                project_name = project.get('name', project)
                print(f"  - {project_name}")
        else:
            print("⚠ 프로젝트를 찾을 수 없습니다.")
        
        # Job 목록 조회
        print("\n[3] Job 목록 조회 중...")
        project_name = "BIDW_ADM"
        jobs = client.get_jobs(project_name)
        if jobs:
            print(f"✓ Job {len(jobs)}개 발견 (프로젝트: {project_name}):")
            for job in jobs[:10]:
                job_name = job.get("name", job)
                print(f"  - {job_name}")
        else:
            print(f"⚠ 프로젝트 '{project_name}'에서 Job을 찾을 수 없습니다.")
        
        print("\n" + "=" * 60)
        print("연결 테스트 완료!")
        print("=" * 60)
        
    else:
        print("✗ 연결 실패!")
        print(f"  - 오류: {result.get('error', 'unknown error')}")
        
        if result.get('details'):
            print("\n  시도한 엔드포인트:")
            for detail in result['details'][:5]:  # 최대 5개만 표시
                url = detail.get('url', 'N/A')
                status = detail.get('status_code', 'N/A')
                error = detail.get('error', '')
                print(f"    - {url} (Status: {status})")
                if error:
                    print(f"      오류: {error}")
        
        print("\n" + "=" * 60)
        print("연결 실패 - 설정을 확인하세요")
        print("=" * 60)


if __name__ == "__main__":
    main()

