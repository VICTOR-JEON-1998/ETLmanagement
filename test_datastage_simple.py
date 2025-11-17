"""DataStage 연결 테스트 (Java SDK 제외)"""

import sys
import os

# Java SDK import 방지
sys.modules['src.datastage.java_sdk_client'] = None

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
        print(f"  - 서버: {client.server_host}:{client.server_port}")
        print(f"  - 사용자: {client.username}")
        
        # Job 목록 조회
        project_name = "BIDW_ADM"
        print(f"\n프로젝트 '{project_name}'의 Job 목록 조회 중...")
        jobs = client.get_jobs(project_name)
        
        if jobs:
            print(f"✓ Job {len(jobs)}개 발견:")
            for job in jobs[:10]:
                print(f"  - {job.get('name', job)}")
            return True, len(jobs)
        else:
            print("⚠ Job을 찾을 수 없습니다.")
            return False, 0
            
    except Exception as e:
        print(f"✗ Designer 클라이언트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False, 0


def test_local_client():
    """로컬 클라이언트 테스트"""
    print("\n" + "=" * 60)
    print("[방법 2] 로컬 DSX 파일 클라이언트 테스트")
    print("=" * 60)
    
    try:
        client = DataStageLocalClient()
        print(f"✓ 로컬 클라이언트 초기화 성공")
        print(f"  - 경로: {client.export_path}")
        
        # Job 목록 조회
        project_name = "BIDW_ADM"
        print(f"\n프로젝트 '{project_name}'의 Job 목록 조회 중...")
        jobs = client.get_jobs(project_name)
        
        if jobs:
            print(f"✓ Job {len(jobs)}개 발견:")
            for job in jobs[:10]:
                print(f"  - {job.get('name', job)}")
            return True, len(jobs)
        else:
            print("⚠ Job을 찾을 수 없습니다.")
            return False, 0
            
    except Exception as e:
        print(f"✗ 로컬 클라이언트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False, 0


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
            try:
                info = client.get_datastage_info()
                if info:
                    print(f"\nDataStage 정보:")
                    print(f"  - 경로: {info.get('datastage_path', 'N/A')}")
                    print(f"  - 버전: {info.get('version', 'N/A')}")
                    print(f"  - 상태: {info.get('status', 'N/A')}")
            except:
                pass
            
            # Job 목록 조회
            project_name = "BIDW_ADM"
            print(f"\n프로젝트 '{project_name}'의 Job 목록 조회 중...")
            jobs = client.get_jobs(project_name)
            
            if jobs:
                print(f"✓ Job {len(jobs)}개 발견:")
                for job in jobs[:10]:
                    print(f"  - {job.get('name', job)}")
                client.close()
                return True, len(jobs)
            else:
                print("⚠ Job을 찾을 수 없습니다.")
                client.close()
                return False, 0
        else:
            print("✗ SSH 연결 실패")
            return False, 0
            
    except Exception as e:
        print(f"✗ SSH 클라이언트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False, 0


def test_rest_api_only():
    """REST API 테스트 (Java SDK 완전 제외)"""
    print("\n" + "=" * 60)
    print("[방법 4] REST API 테스트 (Java SDK 제외)")
    print("=" * 60)
    
    try:
        import requests
        from requests.auth import HTTPBasicAuth
        from urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        
        from src.core.config import get_config
        
        config = get_config().get_datastage_config()
        server_host = config.get("server_host")
        server_port = config.get("server_port", 9446)
        username = config.get("username")
        password = config.get("password")
        
        print(f"✓ REST API 클라이언트 초기화 성공")
        print(f"  - 서버: {server_host}:{server_port}")
        print(f"  - 사용자: {username}")
        
        # 여러 엔드포인트 시도
        base_urls = [
            f"https://{server_host}:{server_port}/ibm/iis/api",
            f"https://{server_host}:{server_port}/ibm/iis/rest",
            f"https://{server_host}:{server_port}/ibm/iis",
            f"https://{server_host}:{server_port}/api",
        ]
        
        endpoints = [
            "v1/health",
            "v1/projects",
            "health",
            "projects",
            "",
        ]
        
        session = requests.Session()
        session.auth = HTTPBasicAuth(username, password)
        session.verify = False
        
        for base_url in base_urls:
            for endpoint in endpoints:
                try:
                    url = f"{base_url}/{endpoint.lstrip('/')}" if endpoint else base_url
                    print(f"\n시도: {url}")
                    response = session.get(url, timeout=10)
                    
                    if response.status_code < 400:
                        print(f"✓ 연결 성공! (Status: {response.status_code})")
                        print(f"  - URL: {url}")
                        
                        # 프로젝트 목록 조회 시도
                        try:
                            projects_url = f"{base_url}/v1/projects"
                            projects_response = session.get(projects_url, timeout=10)
                            if projects_response.status_code < 400:
                                projects = projects_response.json()
                                print(f"\n✓ 프로젝트 조회 성공")
                                if isinstance(projects, dict) and 'projects' in projects:
                                    for project in projects['projects'][:5]:
                                        print(f"  - {project.get('name', project)}")
                        except:
                            pass
                        
                        return True, 0
                except requests.exceptions.SSLError as e:
                    print(f"  SSL 오류: {e}")
                except requests.exceptions.ConnectionError as e:
                    print(f"  연결 오류: {e}")
                except Exception as e:
                    print(f"  오류: {e}")
        
        print("\n✗ 모든 REST API 엔드포인트 실패")
        return False, 0
        
    except Exception as e:
        print(f"✗ REST API 클라이언트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False, 0


def main():
    """모든 방법 테스트"""
    print("=" * 60)
    print("IBM DataStage 연결 테스트 (Java SDK 제외)")
    print("=" * 60)
    
    results = {}
    
    # REST API 테스트
    results['REST API'] = test_rest_api_only()
    
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
    for method, (success, count) in results.items():
        status = f"✓ 성공 ({count}개 Job)" if success else "✗ 실패"
        print(f"  {method:15s}: {status}")
    
    successful_methods = [method for method, (success, _) in results.items() if success]
    if successful_methods:
        print(f"\n✓ 사용 가능한 방법: {', '.join(successful_methods)}")
        print(f"\n✓ DataStage 제어 가능!")
    else:
        print("\n✗ 모든 방법 실패 - 설정을 확인하세요")


if __name__ == "__main__":
    main()

