"""컬럼 추출 테스트 스크립트"""

from pathlib import Path
from src.datastage.dependency_analyzer import DependencyAnalyzer
from src.core.config import get_config

def test_column_extraction():
    """컬럼 추출 테스트"""
    config = get_config()
    ds_config = config.get_datastage_config()
    export_dir = ds_config.get("local_export_path", "")
    
    if not export_dir or not Path(export_dir).exists():
        print(f"Export 디렉토리가 존재하지 않습니다: {export_dir}")
        return
    
    analyzer = DependencyAnalyzer(export_directory=export_dir)
    
    # STYL_CD 컬럼 찾기
    print("\n=== STYL_CD 컬럼 검색 ===\n")
    
    # 방법 1: find_tables_using_column
    tables = analyzer.find_tables_using_column("STYL_CD", export_directory=export_dir)
    print(f"find_tables_using_column 결과: {len(tables)}개 테이블")
    for tbl in tables[:5]:
        print(f"  - {tbl.get('full_name')} (Job: {tbl.get('job_count')}개)")
    
    # 방법 2: find_jobs_using_column_only
    jobs = analyzer.find_jobs_using_column_only("STYL_CD", export_directory=export_dir)
    print(f"\nfind_jobs_using_column_only 결과: {len(jobs)}개 Job")
    for job in jobs[:5]:
        print(f"  - {job.get('job_name')} (테이블: {job.get('table_name')})")
    
    # 방법 3: 전체 의존성 분석에서 확인
    print("\n=== 전체 의존성 분석에서 STYL_CD 검색 ===\n")
    all_deps = analyzer.analyze_all_dependencies(export_directory=export_dir)
    columns_dict = all_deps.get("columns", {})
    
    styl_cd_tables = []
    for table_name, columns in columns_dict.items():
        for col in columns:
            if col.get("name", "").upper() == "STYL_CD":
                styl_cd_tables.append(table_name)
                break
    
    print(f"전체 의존성 분석 결과: {len(styl_cd_tables)}개 테이블에서 STYL_CD 발견")
    for tbl in styl_cd_tables[:10]:
        print(f"  - {tbl}")

if __name__ == "__main__":
    test_column_extraction()

