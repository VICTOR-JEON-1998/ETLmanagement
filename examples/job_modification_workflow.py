"""DataStage Job 수정 워크플로우 예제"""

from src.datastage.job_manager import JobManager
from src.datastage.dsx_editor import DSXEditor
from src.core.logger import get_logger

logger = get_logger(__name__)


def example_table_name_modification():
    """테이블 이름 수정 예제"""
    print("=" * 60)
    print("DataStage Job 수정 워크플로우 예제")
    print("=" * 60)
    
    manager = JobManager()
    project_name = "BIDW_ADM"
    
    # 수정 함수 정의: 테이블 이름 변경
    def modify_table_name(editor: DSXEditor) -> bool:
        """테이블 이름 변경"""
        # 예: 스키마.테이블 이름 변경
        count = editor.replace_table_name(
            old_table="OLD_TABLE_NAME",
            new_table="NEW_TABLE_NAME",
            old_schema="OLD_SCHEMA",
            new_schema="NEW_SCHEMA"
        )
        return count > 0
    
    # 수정 함수 리스트
    modifications = [modify_table_name]
    
    # 전체 워크플로우 실행
    print(f"\n[1] 프로젝트 '{project_name}' Export 시작...")
    result = manager.export_modify_import_workflow(
        project_name=project_name,
        modifications=modifications,
        overwrite=True
    )
    
    # 결과 출력
    print("\n" + "=" * 60)
    print("워크플로우 결과")
    print("=" * 60)
    
    print(f"\n전체 성공: {'✓' if result['success'] else '✗'}")
    
    print(f"\n[Export] {'✓' if result['steps']['export']['success'] else '✗'}")
    if result['steps']['export'].get('error'):
        print(f"  오류: {result['steps']['export']['error']}")
    
    print(f"\n[Modify] {'✓' if result['steps']['modify']['success'] else '✗'}")
    if result['steps']['modify'].get('error'):
        print(f"  오류: {result['steps']['modify']['error']}")
    if result['steps']['modify'].get('modifications_applied'):
        print(f"  적용된 수정: {result['steps']['modify']['modifications_applied']}개")
    
    print(f"\n[Import] {'✓' if result['steps']['import']['success'] else '✗'}")
    if result['steps']['import'].get('error'):
        print(f"  오류: {result['steps']['import']['error']}")
    
    if result.get('error'):
        print(f"\n전체 오류: {result['error']}")


def example_simple_modification():
    """간단한 수정 예제"""
    print("\n" + "=" * 60)
    print("간단한 Job 수정 예제")
    print("=" * 60)
    
    manager = JobManager()
    
    # DSX 파일 경로 (이미 Export된 파일)
    dsx_file = "C:\\Users\\D001_2240052\\Desktop\\DBA-DE\\ETL job files\\BIDW_ADM_AllJobs.dsx"
    
    # Job 로드
    print(f"\n[1] Job 로드: {dsx_file}")
    editor = manager.load_job_from_dsx(dsx_file)
    
    if not editor:
        print("✗ Job 로드 실패")
        return
    
    job_name = editor.get_job_name()
    print(f"✓ Job 로드 성공: {job_name}")
    
    # 테이블 목록 확인
    print(f"\n[2] 테이블 목록 확인...")
    tables = editor.get_all_tables()
    print(f"✓ {len(tables)}개 테이블 발견:")
    for table in tables[:10]:  # 최대 10개만 표시
        schema = table.get('schema', '')
        table_name = table.get('table_name', '')
        print(f"  - {schema}.{table_name}" if schema else f"  - {table_name}")
    
    # 수정 예제: 테이블 이름 변경
    print(f"\n[3] 테이블 이름 수정 예제...")
    # 실제 수정은 주석 처리 (예제용)
    # count = editor.replace_table_name("OLD_TABLE", "NEW_TABLE")
    # print(f"✓ {count}개 테이블 이름 변경")
    
    # 수정된 파일 저장
    print(f"\n[4] 수정된 파일 저장...")
    output_file = dsx_file.replace('.dsx', '_modified.dsx')
    if editor.save(output_file, backup=True):
        print(f"✓ 파일 저장 성공: {output_file}")
        
        # 변경 사항 요약
        changes = editor.get_changes_summary()
        print(f"\n변경 사항 요약:")
        print(f"  - 원본 크기: {changes['original_size']} bytes")
        print(f"  - 현재 크기: {changes['current_size']} bytes")
        print(f"  - 변경 여부: {'예' if changes['has_changes'] else '아니오'}")


def example_import_only():
    """Import만 실행하는 예제"""
    print("\n" + "=" * 60)
    print("Job Import 예제")
    print("=" * 60)
    
    manager = JobManager()
    project_name = "BIDW_ADM"
    
    # 수정된 DSX 파일 경로
    modified_dsx = "C:\\Users\\D001_2240052\\Desktop\\DBA-DE\\ETL job files\\BIDW_ADM_AllJobs_modified.dsx"
    
    print(f"\n[1] Job Import: {modified_dsx}")
    result = manager.import_job(
        dsx_file_path=modified_dsx,
        project_name=project_name,
        overwrite=True
    )
    
    if result["success"]:
        print("✓ Import 성공!")
    else:
        print(f"✗ Import 실패: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    # 예제 1: 전체 워크플로우 (Export → 수정 → Import)
    # example_table_name_modification()
    
    # 예제 2: 간단한 수정
    example_simple_modification()
    
    # 예제 3: Import만 실행
    # example_import_only()
    
    print("\n" + "=" * 60)
    print("예제 완료")
    print("=" * 60)

