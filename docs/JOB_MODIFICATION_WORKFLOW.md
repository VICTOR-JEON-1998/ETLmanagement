# DataStage Job 수정 워크플로우 가이드

## 개요

이 가이드는 Python을 사용하여 DataStage Job을 Export → 수정 → Import하는 전체 워크플로우를 설명합니다.

## 워크플로우 단계

```
1. Export: DataStage에서 전체 Job Export
2. 수정: Python으로 DSX 파일 수정
3. Import: 수정된 Job을 DataStage에 Import
```

## 사용 방법

### 방법 1: 전체 워크플로우 (권장)

```python
from src.datastage.job_manager import JobManager
from src.datastage.dsx_editor import DSXEditor

# Job Manager 생성
manager = JobManager()

# 수정 함수 정의
def modify_tables(editor: DSXEditor) -> bool:
    """테이블 이름 변경"""
    count = editor.replace_table_name(
        old_table="OLD_TABLE",
        new_table="NEW_TABLE",
        old_schema="OLD_SCHEMA",
        new_schema="NEW_SCHEMA"
    )
    return count > 0

# 전체 워크플로우 실행
result = manager.export_modify_import_workflow(
    project_name="BIDW_ADM",
    modifications=[modify_tables],
    overwrite=True
)

if result["success"]:
    print("✓ 워크플로우 완료!")
else:
    print(f"✗ 실패: {result.get('error')}")
```

### 방법 2: 단계별 실행

#### Step 1: Export

```python
from src.datastage.job_manager import JobManager

manager = JobManager()

# 프로젝트 전체 Export
result = manager.export_all_jobs(
    project_name="BIDW_ADM",
    output_file="BIDW_ADM_AllJobs.dsx"
)

if result["success"]:
    print(f"✓ Export 완료: {result.get('file_size', 0)} bytes")
```

#### Step 2: 수정

```python
from src.datastage.dsx_editor import DSXEditor

# DSX 파일 로드
editor = DSXEditor("BIDW_ADM_AllJobs.dsx")

# 테이블 이름 변경
editor.replace_table_name(
    old_table="OLD_TABLE",
    new_table="NEW_TABLE"
)

# 연결 문자열 변경
editor.replace_connection_string(
    old_connection="old_server:port",
    new_connection="new_server:port"
)

# Job 이름 변경
editor.update_job_name("NewJobName")

# 수정된 파일 저장
editor.save("BIDW_ADM_AllJobs_modified.dsx", backup=True)
```

#### Step 3: Import

```python
from src.datastage.job_manager import JobManager

manager = JobManager()

# 수정된 Job Import
result = manager.import_job(
    dsx_file_path="BIDW_ADM_AllJobs_modified.dsx",
    project_name="BIDW_ADM",
    overwrite=True
)

if result["success"]:
    print("✓ Import 완료!")
```

## DSXEditor 주요 기능

### 1. 테이블 이름 변경

```python
editor.replace_table_name(
    old_table="OLD_TABLE",
    new_table="NEW_TABLE",
    old_schema="OLD_SCHEMA",  # 선택사항
    new_schema="NEW_SCHEMA"    # 선택사항
)
```

### 2. 연결 문자열 변경

```python
editor.replace_connection_string(
    old_connection="old_server:port",
    new_connection="new_server:port"
)
```

### 3. Job 이름 변경

```python
editor.update_job_name("NewJobName")
```

### 4. Job 설명 변경

```python
editor.update_description("새로운 설명")
```

### 5. 특정 값 교체

```python
editor.replace_value(
    key="TableName",
    old_value="OLD_TABLE",
    new_value="NEW_TABLE",
    record_identifier="RECORD_ID"  # 선택사항: 특정 레코드만
)
```

### 6. 테이블 정보 조회

```python
tables = editor.get_all_tables()
for table in tables:
    print(f"{table['schema']}.{table['table_name']}")
```

## 수정 함수 예제

### 예제 1: 여러 테이블 일괄 변경

```python
def modify_multiple_tables(editor: DSXEditor) -> bool:
    """여러 테이블 이름 변경"""
    changes = [
        ("OLD_TABLE1", "NEW_TABLE1", "OLD_SCHEMA", "NEW_SCHEMA"),
        ("OLD_TABLE2", "NEW_TABLE2", "OLD_SCHEMA", "NEW_SCHEMA"),
    ]
    
    total_count = 0
    for old_table, new_table, old_schema, new_schema in changes:
        count = editor.replace_table_name(
            old_table, new_table, old_schema, new_schema
        )
        total_count += count
    
    return total_count > 0
```

### 예제 2: 조건부 수정

```python
def modify_conditional(editor: DSXEditor) -> bool:
    """조건에 따라 수정"""
    # 모든 테이블 조회
    tables = editor.get_all_tables()
    
    # 특정 스키마의 테이블만 변경
    count = 0
    for table in tables:
        if table['schema'] == 'OLD_SCHEMA':
            count += editor.replace_table_name(
                table['table_name'],
                table['table_name'].replace('OLD_', 'NEW_'),
                'OLD_SCHEMA',
                'NEW_SCHEMA'
            )
    
    return count > 0
```

### 예제 3: 연결 정보 변경

```python
def modify_connection(editor: DSXEditor) -> bool:
    """데이터베이스 연결 정보 변경"""
    count = editor.replace_connection_string(
        old_connection="10.100.20.50:11433",
        new_connection="10.100.20.60:11433"
    )
    return count > 0
```

## 주의사항

1. **백업**: 수정 전 원본 파일이 자동으로 백업됩니다 (`.dsx.backup`)

2. **덮어쓰기**: Import 시 `overwrite=True`로 설정하면 기존 Job을 덮어씁니다

3. **파일 크기**: 프로젝트가 크면 Export 파일도 큽니다 (수백 MB 가능)

4. **타임아웃**: Export는 최대 10분, Import는 최대 5분 타임아웃

5. **테스트**: Import 전에 수정된 DSX 파일을 검토하세요

## 문제 해결

### Export 실패

- DataStage 서버 연결 확인
- `dsexport.exe` 경로 확인
- 프로젝트 이름 확인

### 수정 실패

- DSX 파일 형식 확인
- 파일 인코딩 확인 (UTF-8)
- 수정할 값이 실제로 존재하는지 확인

### Import 실패

- DataStage 서버 연결 확인
- `dsimport.exe` 경로 확인
- 프로젝트 이름 확인
- DSX 파일이 올바른 형식인지 확인

## 예제 실행

```bash
# 예제 실행
python examples/job_modification_workflow.py
```

## 관련 파일

- `src/datastage/job_manager.py`: 워크플로우 관리
- `src/datastage/dsx_editor.py`: DSX 파일 편집
- `src/datastage/designer_client.py`: Export/Import 기능
- `examples/job_modification_workflow.py`: 사용 예제

