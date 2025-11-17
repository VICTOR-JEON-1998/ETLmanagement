"""테이블/컬럼 변경에 따른 연관 Job 자동 수정 예제"""

from src.datastage.job_modifier import JobModifier
from src.datastage.dependency_analyzer import DependencyAnalyzer
from src.core.config import get_config
from src.core.logger import get_logger

logger = get_logger(__name__)


def example_table_name_change():
    """테이블 이름 변경 예제"""
    print("=" * 60)
    print("테이블 이름 변경 예제")
    print("=" * 60)
    
    config = get_config().get_datastage_config()
    export_path = config.get("local_export_path", "")
    
    modifier = JobModifier(export_directory=export_path)
    
    # 테이블 이름 변경
    result = modifier.modify_table_name(
        old_table="OLD_TABLE_NAME",
        new_table="NEW_TABLE_NAME",
        old_schema="OLD_SCHEMA",
        new_schema="NEW_SCHEMA"
    )
    
    # 리포트 출력
    report = modifier.generate_modification_report(result)
    print(report)
    
    # 수정된 파일 목록
    if result.get("modified_jobs"):
        print("\n수정된 파일 목록:")
        for job in result["modified_jobs"]:
            print(f"  - {job['modified_file']}")


def example_column_name_change():
    """컬럼 이름 변경 예제"""
    print("\n" + "=" * 60)
    print("컬럼 이름 변경 예제")
    print("=" * 60)
    
    config = get_config().get_datastage_config()
    export_path = config.get("local_export_path", "")
    
    modifier = JobModifier(export_directory=export_path)
    
    # 컬럼 이름 변경
    result = modifier.modify_column_name(
        table_name="TABLE_NAME",
        old_column="OLD_COLUMN",
        new_column="NEW_COLUMN",
        schema="SCHEMA_NAME"
    )
    
    # 리포트 출력
    report = modifier.generate_modification_report(result)
    print(report)


def example_find_related_jobs():
    """연관 Job 찾기 예제"""
    print("\n" + "=" * 60)
    print("연관 Job 찾기 예제")
    print("=" * 60)
    
    config = get_config().get_datastage_config()
    export_path = config.get("local_export_path", "")
    
    analyzer = DependencyAnalyzer(export_directory=export_path)
    
    # 특정 테이블을 사용하는 Job 찾기
    table_name = "TABLE_NAME"
    schema = "SCHEMA_NAME"
    
    print(f"\n테이블 '{schema}.{table_name}'을 사용하는 Job 찾기...")
    related_jobs = analyzer.find_jobs_using_table(
        table_name=table_name,
        schema=schema
    )
    
    if related_jobs:
        print(f"\n✓ {len(related_jobs)}개 Job 발견:")
        for job in related_jobs:
            print(f"  - {job['job_name']}")
            print(f"    파일: {job['file_path']}")
    else:
        print("⚠ 관련 Job을 찾을 수 없습니다.")
    
    # 특정 컬럼을 사용하는 Job 찾기
    column_name = "COLUMN_NAME"
    print(f"\n컬럼 '{schema}.{table_name}.{column_name}'을 사용하는 Job 찾기...")
    related_jobs = analyzer.find_jobs_using_column(
        table_name=table_name,
        column_name=column_name,
        schema=schema
    )
    
    if related_jobs:
        print(f"\n✓ {len(related_jobs)}개 Job 발견:")
        for job in related_jobs:
            print(f"  - {job['job_name']}")
            print(f"    파일: {job['file_path']}")
    else:
        print("⚠ 관련 Job을 찾을 수 없습니다.")


def example_full_analysis():
    """전체 의존성 분석 예제"""
    print("\n" + "=" * 60)
    print("전체 의존성 분석 예제")
    print("=" * 60)
    
    config = get_config().get_datastage_config()
    export_path = config.get("local_export_path", "")
    
    analyzer = DependencyAnalyzer(export_directory=export_path)
    
    print("\n전체 Job 의존성 분석 중...")
    all_deps = analyzer.analyze_all_dependencies()
    
    print(f"\n✓ {len(all_deps.get('jobs', []))}개 Job 분석 완료")
    print(f"✓ {len(all_deps.get('tables', {}))}개 테이블 발견")
    
    # 가장 많이 사용되는 테이블
    if all_deps.get("tables"):
        print("\n가장 많이 사용되는 테이블 (상위 10개):")
        table_usage = sorted(
            all_deps["tables"].items(),
            key=lambda x: len(x[1]),
            reverse=True
        )[:10]
        
        for table_name, jobs in table_usage:
            print(f"  - {table_name}: {len(jobs)}개 Job")


def example_workflow():
    """전체 워크플로우 예제"""
    print("\n" + "=" * 60)
    print("전체 워크플로우 예제")
    print("=" * 60)
    
    config = get_config().get_datastage_config()
    export_path = config.get("local_export_path", "")
    
    print("\n[워크플로우]")
    print("1. DataStage Designer에서 프로젝트 Export")
    print("2. Python으로 연관 Job 분석 및 수정")
    print("3. 수정된 Job을 DataStage Designer에서 Import")
    print("")
    
    # 예시: 테이블 이름 변경
    print("[Step 1] 연관 Job 찾기...")
    analyzer = DependencyAnalyzer(export_directory=export_path)
    related_jobs = analyzer.find_jobs_using_table(
        table_name="EXAMPLE_TABLE",
        schema="EXAMPLE_SCHEMA"
    )
    print(f"  ✓ {len(related_jobs)}개 Job 발견")
    
    if related_jobs:
        print("\n[Step 2] Job 수정...")
        modifier = JobModifier(export_directory=export_path)
        result = modifier.modify_table_name(
            old_table="EXAMPLE_TABLE",
            new_table="NEW_EXAMPLE_TABLE",
            old_schema="EXAMPLE_SCHEMA",
            new_schema="NEW_EXAMPLE_SCHEMA"
        )
        
        print("\n[Step 3] 수정 결과:")
        report = modifier.generate_modification_report(result)
        print(report)
        
        print("\n[Step 4] 다음 단계:")
        print("  - DataStage Designer에서 수정된 .modified.dsx 파일들을 Import")
        print("  - Import 전에 수정 내용을 검토하세요")


if __name__ == "__main__":
    # 예제 1: 테이블 이름 변경
    # example_table_name_change()
    
    # 예제 2: 컬럼 이름 변경
    # example_column_name_change()
    
    # 예제 3: 연관 Job 찾기
    example_find_related_jobs()
    
    # 예제 4: 전체 의존성 분석
    # example_full_analysis()
    
    # 예제 5: 전체 워크플로우
    # example_workflow()
    
    print("\n" + "=" * 60)
    print("예제 완료")
    print("=" * 60)

