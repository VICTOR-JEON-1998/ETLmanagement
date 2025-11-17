"""DSX 파일 추출 테스트 스크립트"""

import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.core.config import get_config
from src.core.logger import setup_logger, get_logger
from src.datastage.dsx_parser import DSXParser

# 로거 설정
config = get_config()
logging_config = config.get_logging_config()
setup_logger(
    "etlmanagement",
    level="DEBUG",  # DEBUG 레벨로 설정하여 상세 로그 확인
    log_format=logging_config.get("format"),
    log_file=logging_config.get("file")
)
logger = get_logger(__name__)

def test_dsx_extraction():
    """DSX 파일 추출 테스트"""
    
    # DSX 파일 경로
    dsx_file = Path("Datastage export jobs/exportall.dsx")
    
    if not dsx_file.exists():
        print(f"DSX 파일을 찾을 수 없습니다: {dsx_file}")
        return
    
    print(f"DSX 파일 분석: {dsx_file}\n")
    print("=" * 80)
    
    # DSX 파서 생성
    parser = DSXParser()
    
    # 파일 읽기
    with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # 여러 Job 파싱
    parsed_jobs = parser.parse_multiple_jobs(content, str(dsx_file))
    
    print(f"\n총 {len(parsed_jobs)}개 Job 발견\n")
    
    # m_DM_CUST_a01 Job 찾기
    target_job = None
    for job in parsed_jobs:
        if job.get("name") == "m_DM_CUST_a01":
            target_job = job
            break
    
    if not target_job:
        print("m_DM_CUST_a01 Job을 찾을 수 없습니다.")
        print("\n발견된 Job 목록:")
        for job in parsed_jobs[:5]:  # 처음 5개만
            print(f"  - {job.get('name')}")
        return
    
    print(f"Job: {target_job.get('name')}")
    print(f"Source 테이블: {len(target_job.get('source_tables', []))}개")
    print(f"Target 테이블: {len(target_job.get('target_tables', []))}개")
    
    print("\n[Source 테이블 목록]")
    for i, table in enumerate(target_job.get('source_tables', []), 1):
        full_name = f"{table.get('schema', '')}.{table.get('table_name', '')}" if table.get('schema') else table.get('table_name', '')
        print(f"  {i}. {full_name} (stage: {table.get('stage_name', '')}, type: {table.get('table_type', 'unknown')})")
    
    print("\n[Target 테이블 목록]")
    for i, table in enumerate(target_job.get('target_tables', []), 1):
        full_name = f"{table.get('schema', '')}.{table.get('table_name', '')}" if table.get('schema') else table.get('table_name', '')
        print(f"  {i}. {full_name} (stage: {table.get('stage_name', '')}, type: {table.get('table_type', 'unknown')})")
    
    print("\n" + "=" * 80)
    print("\n로그 파일을 확인하세요:")
    log_file = logging_config.get("file", "etlmanagement.log")
    print(f"  {log_file}")

if __name__ == "__main__":
    test_dsx_extraction()

