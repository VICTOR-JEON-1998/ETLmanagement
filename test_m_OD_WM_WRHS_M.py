"""m_OD_WM_WRHS_M_a01 Job 테이블 추출 테스트"""

import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.datastage.dependency_analyzer import DependencyAnalyzer
from src.core.logger import setup_logger, get_logger

# 로거 설정
setup_logger("etlmanagement", level="DEBUG")
logger = get_logger(__name__)


def test_specific_job():
    """m_OD_WM_WRHS_M_a01 Job 테이블 추출 테스트"""
    export_dir = Path("Datastage export jobs")
    dsx_file = export_dir / "exportall.dsx"
    
    if not dsx_file.exists():
        print(f"파일을 찾을 수 없습니다: {dsx_file}")
        return
    
    print("=" * 60)
    print("m_OD_WM_WRHS_M_a01 Job 테이블 추출 테스트")
    print("=" * 60)
    
    analyzer = DependencyAnalyzer(export_directory=str(export_dir), resolve_parameters=True)
    
    # 전체 분석
    print("전체 의존성 분석 중...")
    all_deps = analyzer.analyze_all_dependencies()
    
    jobs = all_deps.get("jobs", [])
    
    # m_OD_WM_WRHS_M_a01 Job 찾기
    target_job = None
    for job in jobs:
        if job.get("job_name") == "m_OD_WM_WRHS_M_a01":
            target_job = job
            break
    
    if not target_job:
        print("\nm_OD_WM_WRHS_M_a01 Job을 찾을 수 없습니다.")
        return
    
    print(f"\nJob: {target_job.get('job_name')}")
    tables = target_job.get("tables", [])
    source_tables = target_job.get("source_tables", [])
    target_tables = target_job.get("target_tables", [])
    
    print(f"\n원본 테이블 정보:")
    print(f"  - 소스 테이블 수: {len(source_tables)}개")
    print(f"  - 타겟 테이블 수: {len(target_tables)}개")
    print(f"  - 전체 테이블 수: {len(tables)}개")
    
    print(f"\n소스 테이블 상세:")
    for i, table in enumerate(source_tables, 1):
        schema = table.get("schema", "")
        table_name = table.get("table_name", "")
        stage_name = table.get("stage_name", "")
        db_type = table.get("db_type", "")
        full_name = f"{schema}.{table_name}" if schema else table_name
        print(f"  {i}. {full_name}")
        print(f"     - Stage: {stage_name}")
        print(f"     - DB 타입: {db_type}")
        print(f"     - 원본 파라미터: {table.get('original_parameter', 'N/A')}")
    
    print(f"\n타겟 테이블 상세:")
    for i, table in enumerate(target_tables, 1):
        schema = table.get("schema", "")
        table_name = table.get("table_name", "")
        stage_name = table.get("stage_name", "")
        db_type = table.get("db_type", "")
        full_name = f"{schema}.{table_name}" if schema else table_name
        print(f"  {i}. {full_name}")
        print(f"     - Stage: {stage_name}")
        print(f"     - DB 타입: {db_type}")
        print(f"     - 원본 파라미터: {table.get('original_parameter', 'N/A')}")
    
    print(f"\n매핑된 테이블 상세:")
    for i, table in enumerate(tables, 1):
        full_name = table.get("full_name", "")
        db_type = table.get("db_type", "")
        print(f"  {i}. {full_name} ({db_type})")
    
    # 중복 제거 확인
    unique_tables = {}
    for table in tables:
        full_name = table.get("full_name", "")
        if full_name and full_name not in unique_tables:
            unique_tables[full_name] = table
    
    print(f"\n중복 제거 후 고유 테이블 수: {len(unique_tables)}개")
    for full_name, table in unique_tables.items():
        db_type = table.get("db_type", "")
        print(f"  - {full_name} ({db_type})")


if __name__ == "__main__":
    test_specific_job()

