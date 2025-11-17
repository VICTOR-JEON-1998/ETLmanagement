# DataStage Export 가이드

## 개요

SSH나 REST API를 통한 Job 목록 조회가 어려운 경우, DataStage Designer에서 프로젝트를 Export하여 로컬에서 Job 정보를 읽을 수 있습니다.

## Export 방법

### 방법 1: DataStage Designer에서 Export

1. **DataStage Designer 실행**
   - IBM InfoSphere DataStage and QualityStage Designer 실행
   - 프로젝트 `BIDW_ADM`에 연결

2. **프로젝트 Export**
   - 메뉴: `File` → `Export` → `DataStage Components...`
   - 또는 `File` → `Export` → `All`
   - Export 대상 선택:
     - ✅ `Jobs` (모든 Job)
     - ✅ `Sequences` (Sequence 포함)
     - ✅ `Table Definitions` (테이블 정의 포함, 선택사항)
   - Export 형식: `DSX` (DataStage Export)
   - 저장 위치: `C:\Users\D001_2240052\Desktop\DBA-DE\ETL job files`

3. **파일 저장**
   - 파일명 예시: `BIDW_ADM_Export.dsx` 또는 `BIDW_ADM_AllJobs.dsx`
   - 여러 파일로 나누어 Export해도 됨 (프로젝트가 큰 경우)

### 방법 2: 명령줄에서 Export (SSH 가능한 경우)

SSH로 서버에 접근 가능한 경우, `dsexport` 명령어 사용:

```bash
# SSH로 서버 접속
ssh etl_admin@10.100.20.70

# DataStage 환경 설정
source /opt/IBM/InformationServer/Server/DSEngine/dsenv

# 프로젝트 Export
dsexport -project BIDW_ADM -file /tmp/BIDW_ADM_Export.dsx

# 또는 특정 Job만 Export
dsexport -project BIDW_ADM -job JOB_NAME -file /tmp/JOB_NAME.dsx
```

## Export 파일 저장 위치

현재 설정된 경로:
```
C:\Users\D001_2240052\Desktop\DBA-DE\ETL job files
```

이 경로에 `.dsx` 파일을 저장하면 자동으로 인식됩니다.

## Python에서 사용하기

Export한 파일이 저장되면 자동으로 인식됩니다:

```python
from src.datastage import get_datastage_client

client = get_datastage_client()

# Job 목록 조회 (로컬 DSX 파일에서 자동으로 읽음)
jobs = client.get_jobs("BIDW_ADM")
print(f"Job {len(jobs)}개 발견")

for job in jobs:
    print(f"  - {job['name']}")
```

## Export 파일 형식

DataStage Export 파일은 XML 형식입니다:
- 확장자: `.dsx`
- 인코딩: UTF-8 또는 ASCII
- 구조: XML 기반의 DataStage 메타데이터

## 주의사항

1. **파일 크기**: 프로젝트가 크면 Export 파일도 큽니다 (수백 MB 가능)
2. **업데이트**: Job이 변경되면 다시 Export해야 최신 정보를 얻을 수 있습니다
3. **보안**: Export 파일에는 Job 정의와 메타데이터가 포함되므로 보안에 주의하세요

## 현재 상태

✅ 로컬 DSX 파일에서 **1개 Job 발견**: `m_DM_SP_SL_PAY_D_a01`

추가 Export를 통해 더 많은 Job 정보를 얻을 수 있습니다.

