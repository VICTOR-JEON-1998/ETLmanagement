# Job 목록 조회 방법

## 개요

현재 시스템은 **여러 방법을 자동으로 시도**하여 Job 목록을 가져옵니다. Export는 **필수가 아닙니다**.

## 자동 시도 순서

시스템은 다음 순서로 자동으로 시도합니다:

1. **REST API** (DataStage 11.7+)
   - 가장 빠르고 효율적
   - 실시간 데이터

2. **Java SDK** (로컬 클라이언트)
   - 현재 비활성화됨 (Segmentation fault 문제)
   - 활성화하려면 `config.yaml`에서 `java_sdk.enabled: true` 설정

3. **SSH 접근**
   - DataStage 서버에 직접 접근
   - `dsjob` 명령어 사용
   - **Export 불필요**

4. **로컬 DSX 파일** (Fallback)
   - Export 파일이 있는 경우에만 사용
   - **이 방법만 사용하려면 Export 필요**

## 현재 상태

현재는 **로컬 DSX 파일**에서만 Job을 찾고 있습니다. 이는 다음을 의미합니다:

- SSH 연결이 실패했거나
- REST API가 작동하지 않거나
- 다른 방법들이 모두 실패한 경우

## Export 없이 사용하는 방법

### 방법 1: SSH 연결 개선 (권장)

SSH 연결이 작동하면 Export 없이 실시간으로 Job 목록을 가져올 수 있습니다.

```bash
# SSH 연결 테스트
python cli/main.py test-connection
```

SSH 연결이 실패하는 경우:
- 비밀번호 확인
- 네트워크 연결 확인
- 방화벽 설정 확인

### 방법 2: REST API 활성화

DataStage REST API가 활성화되어 있으면 가장 효율적입니다.

### 방법 3: Java SDK 활성화 (선택사항)

로컬 DataStage 클라이언트가 설치되어 있고, Segmentation fault 문제가 해결되면 사용 가능합니다.

## Export가 필요한 경우

다음 경우에만 Export가 필요합니다:

1. **오프라인 작업**: 네트워크 연결이 없는 경우
2. **백업/아카이브**: 특정 시점의 Job 목록을 보관
3. **모든 다른 방법 실패**: SSH, REST API, Java SDK 모두 실패한 경우

## 권장 사항

1. **SSH 연결 확인 및 개선**
   - 가장 실용적인 방법
   - Export 불필요
   - 실시간 데이터

2. **REST API 확인**
   - 가장 빠른 방법
   - DataStage 관리자에게 REST API 활성화 요청

3. **로컬 Export는 보조 수단으로 사용**
   - 주기적으로 Export하여 백업
   - 오프라인 작업 시 사용

## 현재 설정 확인

```bash
# 연결 테스트
python cli/main.py test-connection

# Job 목록 조회 (자동으로 최적의 방법 선택)
python cli/main.py list-jobs --project BIDW_ADM
```

## 결론

**Export는 필수가 아닙니다.** SSH 연결이 작동하면 Export 없이도 실시간으로 Job 목록을 가져올 수 있습니다. 현재는 SSH 연결 문제로 로컬 파일을 사용하고 있지만, SSH 연결을 개선하면 Export 없이 사용할 수 있습니다.

