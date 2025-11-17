"""DataStage 연결 테스트 (Java SDK 우회)"""

from src.datastage.designer_client import DataStageDesignerClient
from src.datastage.local_client import DataStageLocalClient
from src.datastage.ssh_client import DataStageSSHClient
from src.core.logger import get_logger

logger = get_logger(__name__)


def test_designer_client():
    """Designer 클라이언트 테스트"""
    print("\n" + "=" * 60)
    print("[방법 1] Designer 클라이언트 테스트")
    print("=" * 60)
    
    try:
        client = DataStageDesignerClient()
        print(f"✓ Designer 클라이언트 초기화 성공")
        print(f"  - 경로: {client.classic_path}")
        
        # Job 목록 조회
        project_name = "BIDW_ADM"
        print(f"\n프로젝트 '{project_name}'의 Job 목록 조회 중...")
        jobs = client.get_jobs(project_name)
        
        if jobs:
            print(f"✓ Job {len(jobs)}개 발견:")
            for job in jobs[:10]:
                print(f"  - {job.get('name', job)}")
            return True
        else:
            print("⚠ Job을 찾을 수 없습니다.")
            return False
            
    except Exception as e:
        print(f"✗ Designer 클라이언트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_local_client():
    """로컬 클라이언트 테스트"""
    print("\n" + "=" * 60)
    print("[방법 2] 로컬 DSX 파일 클라이언트 테스트")
    print("=" * 60)
    
    try:
        client = DataStageLocalClient()
        print(f"✓ 로컬 클라이언트 초기화 성공")
        
        # Job 목록 조회
        project_name = "BIDW_ADM"
        print(f"\n프로젝트 '{project_name}'의 Job 목록 조회 중...")
        jobs = client.get_jobs(project_name)
        
        if jobs:
            print(f"✓ Job {len(jobs)}개 발견:")
            for job in jobs[:10]:
                print(f"  - {job.get('name', job)}")
            return True
        else:
            print("⚠ Job을 찾을 수 없습니다.")
            return False
            
    except Exception as e:
        print(f"✗ 로컬 클라이언트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_ssh_client():
    """SSH 클라이언트 테스트"""
    print("\n" + "=" * 60)
    print("[방법 3] SSH 클라이언트 테스트")
    print("=" * 60)
    
    try:
        client = DataStageSSHClient()
        print(f"✓ SSH 클라이언트 초기화 성공")
        print(f"  - 호스트: {client.ssh_host}:{client.ssh_port}")
        print(f"  - 사용자: {client.ssh_username}")
        
        print("\nSSH 연결 시도 중...")
        if client.connect():
            print("✓ SSH 연결 성공")
            
            # DataStage 정보 조회
            info = client.get_datastage_info()
            if info:
                print(f"\nDataStage 정보:")
                print(f"  - 경로: {info.get('datastage_path', 'N/A')}")
                print(f"  - 버전: {info.get('version', 'N/A')}")
                print(f"  - 상태: {info.get('status', 'N/A')}")
            
            # Job 목록 조회
            project_name = "BIDW_ADM"
            print(f"\n프로젝트 '{project_name}'의 Job 목록 조회 중...")
            jobs = client.get_jobs(project_name)
            
            if jobs:
                print(f"✓ Job {len(jobs)}개 발견:")
                for job in jobs[:10]:
                    print(f"  - {job.get('name', job)}")
            
            client.close()
            return True
        else:
            print("✗ SSH 연결 실패")
            return False
            
    except Exception as e:
        print(f"✗ SSH 클라이언트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_rest_api():
    """REST API 테스트"""
    print("\n" + "=" * 60)
    print("[방법 4] REST API 테스트")
    print("=" * 60)
    
    try:
        from src.datastage.api_client import DataStageAPIClient
        
        client = DataStageAPIClient()
        print(f"✓ REST API 클라이언트 초기화 성공")
        print(f"  - 서버: {client.server_host}:{client.server_port}")
        print(f"  - URL: {client.api_base_url}")
        
        # 연결 테스트 (Java SDK 제외)
        print("\n연결 테스트 중...")
        result = client.test_connection()
        
        if result["success"]:
            print(f"✓ 연결 성공!")
            print(f"  - 방법: {result.get('method', 'unknown')}")
            
            # 프로젝트 목록 조회
            projects = client.get_projects()
            if projects:
                print(f"\n✓ 프로젝트 {len(projects)}개 발견:")
                for project in projects[:5]:
                    print(f"  - {project.get('name', project)}")
            
            # Job 목록 조회
            project_name = "BIDW_ADM"
            jobs = client.get_jobs(project_name)
            if jobs:
                print(f"\n✓ Job {len(jobs)}개 발견:")
                for job in jobs[:10]:
                    print(f"  - {job.get('name', job)}")
            
            return True
        else:
            print(f"✗ 연결 실패: {result.get('error', 'unknown error')}")
            return False
            
    except Exception as e:
        print(f"✗ REST API 클라이언트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """모든 방법 테스트"""
    print("=" * 60)
    print("IBM DataStage 연결 테스트 (다양한 방법)")
    print("=" * 60)
    
    results = {}
    
    # REST API 테스트 (Java SDK 우회)
    results['REST API'] = test_rest_api()
    
    # Designer 클라이언트 테스트
    results['Designer'] = test_designer_client()
    
    # 로컬 클라이언트 테스트
    results['Local'] = test_local_client()
    
    # SSH 클라이언트 테스트
    results['SSH'] = test_ssh_client()
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("테스트 결과 요약")
    print("=" * 60)
    for method, success in results.items():
        status = "✓ 성공" if success else "✗ 실패"
        print(f"  {method:15s}: {status}")
    
    successful_methods = [method for method, success in results.items() if success]
    if successful_methods:
        print(f"\n✓ 사용 가능한 방법: {', '.join(successful_methods)}")
    else:
        print("\n✗ 모든 방법 실패 - 설정을 확인하세요")


if __name__ == "__main__":
    main()

