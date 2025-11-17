# 테이블/컬럼 변경에 따른 Job 자동 수정 워크플로우

## 개요

이 가이드는 테이블이나 컬럼이 변경되었을 때, 연관된 DataStage Job들을 자동으로 찾아서 수정하는 워크플로우를 설명합니다.

## 워크플로우

```
1. Export: DataStage Designer에서 프로젝트 Export
2. 분석: Python으로 연관 Job 찾기
3. 수정: Python으로 Job 자동 수정
4. Import: DataStage Designer에서 수정된 Job Import
```

## 사용 방법

### Step 1: DataStage Designer에서 Export

1. DataStage Designer 실행
2. 프로젝트 `BIDW_ADM` 연결
3. `File` → `Export` → `DataStage Components...`
4. Export 대상 선택:
   - ✅ `Jobs` (모든 Job)
   - ✅ `Sequences` (Sequence 포함)
5. Export 파일 저장:
   ```
   C:\Users\D001_2240052\Desktop\DBA-DE\ETL job files
   ```

### Step 2: Python으로 연관 Job 찾기 및 수정

#### 예제 1: 테이블 이름 변경

```python
from src.datastage.job_modifier import JobModifier
from src.core.config import get_config

config = get_config().get_datastage_config()
export_path = config.get("local_export_path", "")

# Job 수정기 생성
modifier = JobModifier(export_directory=export_path)

# 테이블 이름 변경에 따른 연관 Job 수정
result = modifier.modify_table_name(
    old_table="OLD_TABLE_NAME",
    new_table="NEW_TABLE_NAME",
    old_schema="OLD_SCHEMA",
    new_schema="NEW_SCHEMA"
)

# 수정 결과 리포트
report = modifier.generate_modification_report(result)
print(report)
```

#### 예제 2: 컬럼 이름 변경

```python
from src.datastage.job_modifier import JobModifier

modifier = JobModifier(export_directory=export_path)

# 컬럼 이름 변경에 따른 연관 Job 수정
result = modifier.modify_column_name(
    table_name="TABLE_NAME",
    old_column="OLD_COLUMN",
    new_column="NEW_COLUMN",
    schema="SCHEMA_NAME"
)

# 리포트 출력
report = modifier.generate_modification_report(result)
print(report)
```

#### 예제 3: 연관 Job만 찾기 (수정 전 확인)

```python
from src.datastage.dependency_analyzer import DependencyAnalyzer

analyzer = DependencyAnalyzer(export_directory=export_path)

# 특정 테이블을 사용하는 Job 찾기
related_jobs = analyzer.find_jobs_using_table(
    table_name="TABLE_NAME",
    schema="SCHEMA_NAME"
)

print(f"연관 Job {len(related_jobs)}개 발견:")
for job in related_jobs:
    print(f"  - {job['job_name']}")
    print(f"    파일: {job['file_path']}")
```

### Step 3: DataStage Designer에서 Import

1. DataStage Designer 실행
2. 프로젝트 `BIDW_ADM` 연결
3. `File` → `Import` → `DataStage Components...`
4. 수정된 `.modified.dsx` 파일 선택
5. Import 옵션:
   - ✅ `Overwrite existing objects` (기존 Job 덮어쓰기)
6. Import 실행

## 주요 기능

### 1. 의존성 분석 (`DependencyAnalyzer`)

#### 테이블을 사용하는 Job 찾기

```python
analyzer = DependencyAnalyzer(export_directory=export_path)

related_jobs = analyzer.find_jobs_using_table(
    table_name="TABLE_NAME",
    schema="SCHEMA_NAME"
)
```

#### 컬럼을 사용하는 Job 찾기

```python
related_jobs = analyzer.find_jobs_using_column(
    table_name="TABLE_NAME",
    column_name="COLUMN_NAME",
    schema="SCHEMA_NAME"
)
```

#### 전체 의존성 분석

```python
all_deps = analyzer.analyze_all_dependencies()

# 가장 많이 사용되는 테이블 찾기
for table_name, jobs in all_deps["tables"].items():
    print(f"{table_name}: {len(jobs)}개 Job")
```

### 2. Job 자동 수정 (`JobModifier`)

#### 테이블 이름 변경

```python
modifier = JobModifier(export_directory=export_path)

result = modifier.modify_table_name(
    old_table="OLD_TABLE",
    new_table="NEW_TABLE",
    old_schema="OLD_SCHEMA",
    new_schema="NEW_SCHEMA"
)
```

#### 컬럼 이름 변경

```python
result = modifier.modify_column_name(
    table_name="TABLE_NAME",
    old_column="OLD_COLUMN",
    new_column="NEW_COLUMN",
    schema="SCHEMA_NAME"
)
```

#### 컬럼 삭제 (경고만)

```python
result = modifier.delete_column(
    table_name="TABLE_NAME",
    column_name="COLUMN_NAME",
    schema="SCHEMA_NAME"
)
# 주의: 컬럼 삭제는 수동 검토 필요
```

## 수정 결과 리포트

수정 후 자동으로 리포트가 생성됩니다:

```
============================================================
DataStage Job 수정 리포트
============================================================

전체 성공: ✓
수정된 Job 수: 5
실패한 Job 수: 0
총 변경사항: 12개

수정된 Job 목록:
------------------------------------------------------------
  ✓ Job1
    원본: BIDW_ADM_AllJobs.dsx
    수정: BIDW_ADM_AllJobs.modified.dsx
    변경: 3개
  ...
============================================================
```

## 실제 사용 시나리오

### 시나리오 1: 테이블 이름 변경

**상황**: `ERP.ORDERS` 테이블이 `ERP.ORDER_MASTER`로 변경됨

```python
from src.datastage.job_modifier import JobModifier
from src.core.config import get_config

config = get_config().get_datastage_config()
modifier = JobModifier(export_directory=config.get("local_export_path"))

# 1. 연관 Job 찾기 및 수정
result = modifier.modify_table_name(
    old_table="ORDERS",
    new_table="ORDER_MASTER",
    old_schema="ERP",
    new_schema="ERP"
)

# 2. 리포트 확인
report = modifier.generate_modification_report(result)
print(report)

# 3. 수정된 파일 목록
for job in result["modified_jobs"]:
    print(f"Import 필요: {job['modified_file']}")
```

### 시나리오 2: 컬럼 이름 변경

**상황**: `ERP.ORDERS.ORDER_DATE` 컬럼이 `ORDER_DT`로 변경됨

```python
# 1. 연관 Job 찾기 및 수정
result = modifier.modify_column_name(
    table_name="ORDERS",
    old_column="ORDER_DATE",
    new_column="ORDER_DT",
    schema="ERP"
)

# 2. 수정 결과 확인
print(f"수정된 Job: {len(result['modified_jobs'])}개")
```

### 시나리오 3: 컬럼 삭제

**상황**: `ERP.ORDERS.OLD_COLUMN` 컬럼 삭제

```python
# 주의: 컬럼 삭제는 수동 검토 필요
result = modifier.delete_column(
    table_name="ORDERS",
    column_name="OLD_COLUMN",
    schema="ERP"
)

# 경고 확인
for warning in result["warnings"]:
    print(f"⚠ {warning['job_name']}: {warning['message']}")
```

## 주의사항

1. **백업**: 수정 전 원본 파일이 자동으로 백업됩니다 (`.dsx.backup`)

2. **검토**: Import 전에 수정된 DSX 파일을 검토하세요

3. **컬럼 삭제**: 컬럼 삭제는 자동 수정이 어려우므로 수동 검토가 필요합니다

4. **복잡한 변경**: 복잡한 변경사항은 수동으로 검토하고 수정하세요

5. **테스트**: Import 후 Job이 정상 작동하는지 테스트하세요

## 파일 구조

수정 후 파일 구조:
```
ETL job files/
├── BIDW_ADM_AllJobs.dsx          # 원본 Export 파일
├── BIDW_ADM_AllJobs.dsx.backup    # 자동 백업
├── BIDW_ADM_AllJobs.modified.dsx  # 수정된 파일 (Import 대상)
└── ...
```

## 문제 해결

### 연관 Job을 찾을 수 없음

- Export 파일이 올바른 위치에 있는지 확인
- 테이블/컬럼 이름이 정확한지 확인 (대소문자 구분)

### 수정이 적용되지 않음

- DSX 파일 형식 확인
- 테이블/컬럼 이름이 실제로 사용되는지 확인

### Import 실패

- DSX 파일이 올바른 형식인지 확인
- DataStage Designer에서 직접 Import 시도
- 오류 메시지 확인

## 관련 파일

- `src/datastage/dependency_analyzer.py`: 의존성 분석
- `src/datastage/job_modifier.py`: Job 자동 수정
- `src/datastage/dsx_editor.py`: DSX 파일 편집
- `examples/table_column_modification.py`: 사용 예제

## 예제 실행

```bash
# 연관 Job 찾기 예제
python examples/table_column_modification.py
```

