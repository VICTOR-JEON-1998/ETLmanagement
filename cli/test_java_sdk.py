"""DataStage Java SDK 연결 테스트 스크립트"""

import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.datastage.java_sdk_client import DataStageJavaSDKClient
from src.core.logger import get_logger

logger = get_logger(__name__)


def main():
    """Java SDK 연결 테스트"""
    print("=" * 60)
    print("DataStage Java SDK 연결 테스트")
    print("=" * 60)
    print()
    
    try:
        client = DataStageJavaSDKClient()
        
        print("1. 연결 테스트 중...")
        result = client.test_connection()
        
        print(f"\n결과:")
        print(f"  성공: {result['success']}")
        print(f"  방법: {result['method']}")
        
        if result.get('java_home'):
            print(f"  Java 경로: {result['java_home']}")
        
        if result.get('jar_files'):
            print(f"  JAR 파일 수: {len(result['jar_files'])}")
            print(f"  JAR 파일 목록:")
            for jar in result['jar_files'][:5]:  # 처음 5개만 표시
                print(f"    - {jar}")
            if len(result['jar_files']) > 5:
                print(f"    ... 외 {len(result['jar_files']) - 5}개")
        
        if result.get('error'):
            print(f"  오류: {result['error']}")
        
        if result['success']:
            print("\n2. 서버 연결 시도 중...")
            if client.connect():
                print("  ✓ 서버 연결 성공")
                
                print("\n3. 프로젝트 목록 조회 중...")
                projects = client.get_projects()
                print(f"  프로젝트 수: {len(projects)}")
                for project in projects[:10]:  # 처음 10개만 표시
                    print(f"    - {project}")
                
                print("\n4. Job 목록 조회 중 (BIDW_ADM)...")
                jobs = client.get_jobs("BIDW_ADM")
                print(f"  Job 수: {len(jobs)}")
                for job in jobs[:10]:  # 처음 10개만 표시
                    print(f"    - {job}")
                
                client.close()
            else:
                print("  ✗ 서버 연결 실패")
        
    except ImportError as e:
        print(f"\n✗ 오류: {e}")
        print("\nJPype1을 설치하세요:")
        print("  pip install JPype1")
    except Exception as e:
        print(f"\n✗ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

