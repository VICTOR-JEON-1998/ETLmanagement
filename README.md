# ETL Management System

IBM DataStage 메타데이터 자동화 및 파이프라인 무결성 시스템

## 프로젝트 개요

ETL Job의 라이프사이클 전반에 걸쳐 DBA/Data Engineer의 작업을 지원하는 자동화 시스템입니다.

### 주요 기능

1. **메타데이터 통합 관리 및 전파 모듈**
   - 영향도 분석: 변경하려는 컬럼/테이블을 사용 중인 모든 DataStage Job 목록 조회
   - 메타데이터 일괄 전파: 컬럼 변경사항을 관련 Job에 자동 반영

2. **DB/ETL 스키마 무결성 보장 모듈**
   - 물리적 불일치 사전 검증 (Trimming Check, PK/Unique 무결성 로직)
   - DDL 3단계 자동화 (PK 해제 → 컬럼 변경 → PK 재생성)

3. **통합 오류 진단 및 모니터링 모듈**
   - 통합 오류 파싱 (SQLSTATE 코드 감지)
   - Lock/성능 실시간 진단

4. **DB 테이블 및 ETL Job 조회**
   - DB별 모든 테이블 목록 조회 (MSSQL, Vertica)
   - ETL Job 목록 조회 및 Job 정의 불러오기

## 기술 스택

- Python 3.8+
- IBM DataStage REST API / SSH
- MSSQL (pyodbc)
- Vertica (vertica-python)
- SSH (paramiko)

## 설치

```bash
# 가상 환경 생성 (선택사항)
python -m venv .venv
.venv\Scripts\activate  # Windows

# 의존성 설치
pip install -r requirements.txt
```

## 설정

1. 환경 변수 파일 생성:
```bash
# Windows
copy config\.env.example config\.env

# Linux/Mac
cp config/.env.example config/.env
```

2. `config/.env` 파일에 실제 비밀번호 입력:
```
DS_PASSWORD=Fila2023!
MSSQL_PASSWORD=MistoSQL2025!
VERTICA_PASSWORD=veradmin1!
SSH_PASSWORD=Fila2023!
```

3. `config/config.yaml` 파일 확인 및 수정 (필요시)

## 사용법

### CLI 명령어

```bash
# 도움말 보기
python cli/main.py --help

# 연결 테스트
python cli/main.py test-connection

# 테이블 목록 조회
python cli/main.py list-tables --db-type mssql
python cli/main.py list-tables --db-type mssql --schema dbo
python cli/main.py list-tables --db-type vertica

# Job 목록 조회
python cli/main.py list-jobs --project BIDW_ADM

# Job 정의 불러오기
python cli/main.py get-job --project BIDW_ADM --job-name "JOB_NAME"

# 영향도 분석
python cli/main.py impact-analysis --project "BIDW_ADM" --table "TABLE_NAME" --schema "SCHEMA"

# 메타데이터 일괄 전파 (Dry Run)
python cli/main.py propagate-metadata --project "BIDW_ADM" --table "TABLE_NAME" --column "COLUMN_NAME" --new-length 100 --dry-run

# 스키마 무결성 검증
python cli/main.py validate-schema --project "BIDW_ADM" --job-name "JOB_NAME" --db-type mssql

# DDL 자동 생성
python cli/main.py generate-ddl --table "TABLE_NAME" --schema "SCHEMA" --column "COLUMN_NAME" --new-length 200 --db-type mssql

# 로그 분석
python cli/main.py parse-logs --log-file "path/to/logfile.log"

# Lock 상태 진단
python cli/main.py diagnose-locks --db-type mssql

# DataStage 경로 디버깅
python cli/debug_datastage.py
```

### GUI 사용

```bash
python gui/main.py
```

**GUI 탭 구성:**
1. **연결 테스트** - DB 및 DataStage 연결 확인
2. **테이블 목록** - DB별 모든 테이블 조회 (트리뷰)
3. **Job 목록** - ETL Job 목록 조회 및 불러오기
4. **영향도 분석** - 테이블/컬럼 변경 영향 분석
5. **스키마 검증** - ETL Job과 DB 스키마 비교
6. **Lock 진단** - 실시간 Lock 상태 확인
7. **로그 분석** - ETL 로그 파싱 및 오류 진단
8. **DDL 생성** - 컬럼 변경 DDL 자동 생성

### 사용 예제

#### 1. 테이블 목록 조회
```bash
# MSSQL의 모든 테이블 조회
python cli/main.py list-tables --db-type mssql

# 특정 스키마의 테이블만 조회
python cli/main.py list-tables --db-type mssql --schema dbo

# 결과를 JSON 파일로 저장
python cli/main.py list-tables --db-type mssql --output tables.json
```

#### 2. Job 목록 조회 및 불러오기
```bash
# Job 목록 조회
python cli/main.py list-jobs --project BIDW_ADM

# Job 정의 불러오기
python cli/main.py get-job --project BIDW_ADM --job-name "JOB_NAME" --output job.dsx
```

#### 3. 영향도 분석
특정 테이블을 사용하는 모든 Job을 찾아보기:
```bash
python cli/main.py impact-analysis \
  --project "BIDW_ADM" \
  --table "CUSTOMER" \
  --schema "dbo" \
  --output impact_report.json
```

## 프로젝트 구조

```
etlmanagement/
├── src/
│   ├── datastage/          # DataStage 연동 모듈
│   │   ├── api_client.py   # REST API 클라이언트
│   │   ├── ssh_client.py   # SSH 클라이언트
│   │   ├── job_parser.py   # Job 메타데이터 파싱
│   │   └── metadata_manager.py  # 메타데이터 일괄 전파
│   ├── database/           # DB 연결 및 스키마 검증 모듈
│   │   ├── connectors.py  # MSSQL/Vertica 연결
│   │   ├── schema_validator.py  # 스키마 무결성 검증
│   │   └── ddl_generator.py     # DDL 자동 생성
│   ├── monitoring/         # 오류 진단 및 모니터링 모듈
│   │   ├── log_parser.py   # ETL 로그 파싱
│   │   ├── error_detector.py   # 오류 코드 감지
│   │   └── lock_diagnostic.py  # Lock/성능 진단
│   └── core/               # 핵심 유틸리티
│       ├── config.py       # 설정 관리
│       └── logger.py        # 로깅 설정
├── config/                 # 설정 파일
│   ├── config.yaml         # 애플리케이션 설정
│   └── .env.example        # 환경 변수 템플릿
├── cli/                    # CLI 인터페이스
│   ├── main.py             # CLI 진입점
│   └── debug_datastage.py  # DataStage 디버깅 스크립트
├── gui/                    # GUI 인터페이스
│   └── main.py             # GUI 진입점
├── tests/                  # 테스트 코드
├── requirements.txt        # Python 의존성
└── README.md
```

## 개발 상태

- [x] Phase 1: 기본 인프라 구축
- [x] Phase 2: 메타데이터 관리 모듈
- [x] Phase 3: 스키마 무결성 모듈
- [x] Phase 4: 모니터링 및 진단 모듈
- [x] Phase 5: CLI 통합 및 문서화
- [x] GUI 인터페이스
- [x] DB 테이블 조회 기능
- [x] ETL Job 조회 기능

## 주의사항

1. **DataStage REST API**: DataStage 11.7+ 버전에서 REST API를 지원합니다. 이전 버전의 경우 SSH를 통해 접근합니다.

2. **SSH 접근 권한**: DataStage 프로젝트에 접근하려면 `dsadm` 계정 권한이 필요할 수 있습니다. `useradmin` 계정으로 접근이 안 되는 경우 `config/config.yaml`에서 SSH 사용자명을 `dsadm`으로 변경하세요.

3. **데이터베이스 연결**: MSSQL과 Vertica에 대한 ODBC 드라이버가 설치되어 있어야 합니다.

4. **보안**: `.env` 파일에는 민감한 정보가 포함되어 있으므로 Git에 커밋하지 마세요.

5. **Dry Run**: 메타데이터 전파 시 기본적으로 `--dry-run` 모드로 실행되므로, 실제 변경 전에 시뮬레이션을 확인하세요.

## 문제 해결

### 연결 오류
```bash
# 연결 테스트 실행
python cli/main.py test-connection

# DataStage 경로 디버깅
python cli/debug_datastage.py
```

### Job 목록이 비어있는 경우
1. SSH 접근 권한 확인: `dsadm` 계정으로 접근해야 할 수 있습니다.
2. 프로젝트 경로 확인: `python cli/debug_datastage.py` 실행
3. 로컬 DataStage 클라이언트를 통해 프로젝트 경로 확인

### 로그 확인
로그 파일은 `logs/etlmanagement.log`에 저장됩니다 (설정에 따라 다를 수 있음).

## 라이선스

프로젝트용 (내부 사용)
