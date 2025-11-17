"""SSH를 통한 Job 목록 빠른 조회 테스트"""

from src.datastage.ssh_client import DataStageSSHClient
from src.core.logger import get_logger
import signal
import sys

logger = get_logger(__name__)

# 타임아웃 설정 (30초)
def timeout_handler(signum, frame):
    raise TimeoutError("Job 목록 조회 타임아웃")

def main():
    print("=" * 60)
    print("SSH를 통한 Job 목록 조회 테스트 (30초 타임아웃)")
    print("=" * 60)
    
    client = DataStageSSHClient()
    
    print(f"\nSSH 연결 정보:")
    print(f"  - 호스트: {client.ssh_host}:{client.ssh_port}")
    print(f"  - 사용자: {client.ssh_username}")
    
    print("\n[1] SSH 연결 중...")
    if not client.connect():
        print("✗ SSH 연결 실패")
        return
    
    print("✓ SSH 연결 성공")
    
    # 타임아웃 설정 (Windows에서는 signal이 제한적이므로 try-except 사용)
    project_name = "BIDW_ADM"
    print(f"\n[2] 프로젝트 '{project_name}'의 Job 목록 조회 중...")
    print("    (최대 30초 대기)")
    
    try:
        import threading
        import queue
        
        result_queue = queue.Queue()
        
        def get_jobs_thread():
            try:
                jobs = client.get_jobs(project_name)
                result_queue.put(("success", jobs))
            except Exception as e:
                result_queue.put(("error", str(e)))
        
        thread = threading.Thread(target=get_jobs_thread, daemon=True)
        thread.start()
        thread.join(timeout=30)  # 30초 타임아웃
        
        if thread.is_alive():
            print("✗ 타임아웃 발생 (30초 초과)")
            print("\n💡 해결 방법:")
            print("   1. DataStage Designer에서 프로젝트를 Export하세요")
            print("   2. Export한 DSX 파일을 다음 경로에 저장하세요:")
            export_path = 'C:\\Users\\D001_2240052\\Desktop\\DBA-DE\\ETL job files'
            print(f"      {export_path}")
            print("   3. 로컬 DSX 파일 파싱 방법을 사용하세요")
            client.close()
            return
        
        if not result_queue.empty():
            status, data = result_queue.get()
            if status == "success":
                jobs = data
                if jobs:
                    print(f"✓ Job {len(jobs)}개 발견:")
                    for i, job in enumerate(jobs[:20], 1):  # 최대 20개만 표시
                        print(f"  {i:3d}. {job.get('name', job)}")
                    if len(jobs) > 20:
                        print(f"  ... 외 {len(jobs) - 20}개")
                    print(f"\n✓ 총 {len(jobs)}개 Job 발견!")
                else:
                    print("⚠ Job을 찾을 수 없습니다.")
            else:
                print(f"✗ 오류 발생: {data}")
        else:
            print("✗ 결과를 받지 못했습니다.")
            
    except Exception as e:
        print(f"✗ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print("\n✓ SSH 연결 종료")


if __name__ == "__main__":
    main()

