"""IBM DataStage Python 연동 예제"""

from src.datastage import get_datastage_client, get_java_sdk_client
from src.core.logger import get_logger

logger = get_logger(__name__)


def example_basic_usage():
    """기본 사용 예제 - 통합 클라이언트 사용"""
    print("=" * 60)
    print("DataStage 기본 사용 예제")
    print("=" * 60)
    
    # 통합 클라이언트 생성 (자동으로 최적의 방법 선택)
    client = get_datastage_client()
    
    # 연결 테스트
    print("\n1. 연결 테스트...")
    result = client.test_connection()
    if result["success"]:
        print(f"   ✓ 연결 성공 (방법: {result.get('method', 'unknown')})")
    else:
        print(f"   ✗ 연결 실패: {result.get('error', 'unknown error')}")
        return
    
    # 프로젝트 목록 조회
    print("\n2. 프로젝트 목록 조회...")
    projects = client.get_projects()
    if projects:
        print(f"   ✓ 프로젝트 {len(projects)}개 발견:")
        for project in projects[:5]:  # 최대 5개만 표시
            print(f"     - {project.get('name', project)}")
    else:
        print("   ✗ 프로젝트를 찾을 수 없습니다.")
        return
    
    # Job 목록 조회
    print("\n3. Job 목록 조회...")
    project_name = "BIDW_ADM"  # 기본 프로젝트
    jobs = client.get_jobs(project_name)
    if jobs:
        print(f"   ✓ Job {len(jobs)}개 발견 (프로젝트: {project_name}):")
        for job in jobs[:10]:  # 최대 10개만 표시
            job_name = job.get("name", job)
            print(f"     - {job_name}")
    else:
        print(f"   ✗ 프로젝트 '{project_name}'에서 Job을 찾을 수 없습니다.")


def example_java_sdk_direct():
    """Java SDK 직접 사용 예제 (고급)"""
    print("\n" + "=" * 60)
    print("DataStage Java SDK 직접 사용 예제")
    print("=" * 60)
    
    try:
        # Java SDK 클라이언트 직접 생성
        java_client = get_java_sdk_client()
        
        # 연결 테스트
        print("\n1. Java SDK 연결 테스트...")
        result = java_client.test_connection()
        if result["success"]:
            print(f"   ✓ Java SDK 연결 성공")
            print(f"   - Java Home: {result.get('java_home', 'N/A')}")
            print(f"   - JAR 파일: {len(result.get('jar_files', []))}개")
        else:
            print(f"   ✗ Java SDK 연결 실패: {result.get('error', 'unknown error')}")
            return
        
        # 서버 연결
        print("\n2. DataStage 서버 연결...")
        if java_client.connect():
            print("   ✓ 서버 연결 성공")
            
            # 프로젝트 목록 조회
            print("\n3. 프로젝트 목록 조회...")
            projects = java_client.get_projects()
            if projects:
                print(f"   ✓ 프로젝트 {len(projects)}개 발견:")
                for project in projects[:5]:
                    print(f"     - {project.get('name', project)}")
            
            # Job 목록 조회
            print("\n4. Job 목록 조회...")
            project_name = "BIDW_ADM"
            jobs = java_client.get_jobs(project_name)
            if jobs:
                print(f"   ✓ Job {len(jobs)}개 발견:")
                for job in jobs[:10]:
                    print(f"     - {job.get('name', job)}")
            
            # 연결 종료
            java_client.close()
            print("\n   ✓ 연결 종료")
        else:
            print("   ✗ 서버 연결 실패")
            
    except ImportError as e:
        print(f"   ✗ JPype1이 설치되어 있지 않습니다: {e}")
        print("   설치 방법: pip install JPype1")
    except Exception as e:
        print(f"   ✗ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


def example_job_definition():
    """Job 정의 조회 예제"""
    print("\n" + "=" * 60)
    print("Job 정의 조회 예제")
    print("=" * 60)
    
    client = get_datastage_client()
    
    project_name = "BIDW_ADM"
    job_name = None  # 실제 Job 이름으로 변경 필요
    
    # 먼저 Job 목록 조회
    print(f"\n프로젝트 '{project_name}'의 Job 목록...")
    jobs = client.get_jobs(project_name)
    if not jobs:
        print("   Job을 찾을 수 없습니다.")
        return
    
    # 첫 번째 Job의 정의 조회
    if jobs:
        job_name = jobs[0].get("name")
        print(f"\nJob 정의 조회: {job_name}...")
        job_def = client.get_job_definition(project_name, job_name)
        if job_def:
            print(f"   ✓ Job 정의 조회 성공")
            print(f"   - Job 이름: {job_def.get('name', job_name)}")
            print(f"   - 프로젝트: {job_def.get('project', project_name)}")
        else:
            print(f"   ✗ Job 정의를 찾을 수 없습니다.")


if __name__ == "__main__":
    # 기본 사용 예제
    example_basic_usage()
    
    # Java SDK 직접 사용 예제 (선택사항)
    # example_java_sdk_direct()
    
    # Job 정의 조회 예제 (선택사항)
    # example_job_definition()
    
    print("\n" + "=" * 60)
    print("예제 실행 완료")
    print("=" * 60)

