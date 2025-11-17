# DataStage Java SDK 연동 가이드

## 개요

이 프로젝트는 IBM DataStage Java SDK를 Python에서 호출하여 DataStage와 연동하는 기능을 제공합니다.

## 현재 상태

### 완료된 작업
- ✅ JPype 라이브러리 통합
- ✅ DataStage JAR 파일 자동 검색 (135개 발견)
- ✅ IBM JDK 경로 자동 감지
- ✅ JVM 시작 로직 구현
- ✅ API 클라이언트 통합 (REST API → Java SDK → SSH 우선순위)

### 알려진 문제
- ⚠️ JVM 시작 시 Segmentation fault 발생 가능 (IBM JDK와 JPype 호환성 문제)
- ⚠️ 실제 DataStage SDK API 메서드 호출 구현 필요

## 해결 방법

### 방법 1: REST API 사용 (권장)
DataStage REST API가 지원되는 경우, Java SDK 대신 REST API를 사용하는 것이 더 안정적입니다.

### 방법 2: SSH를 통한 접근
SSH를 통해 DataStage 서버에 직접 접근하여 `dsjob` 명령어를 사용할 수 있습니다.

### 방법 3: 로컬 Export 파일 파싱
DataStage에서 Export한 `.dsx` 파일을 로컬에서 파싱하여 Job 정보를 추출할 수 있습니다.

## 설정

`config/config.yaml`에서 다음 설정을 확인하세요:

```yaml
datastage:
  java_sdk:
    enabled: true
    client_path: "C:\\IBM\\InformationServer\\ASBNode\\lib\\java"
    java_home: "C:\\IBM\\InformationServer\\jdk"
```

## 사용법

Java SDK는 자동으로 시도되며, 실패 시 SSH 또는 로컬 파일 파싱으로 자동 전환됩니다.

```python
from src.datastage.api_client import DataStageAPIClient

client = DataStageAPIClient()
jobs = client.get_jobs("BIDW_ADM")  # 자동으로 최적의 방법 선택
```

## 문제 해결

### Segmentation fault 발생 시
1. Java SDK 비활성화: `config.yaml`에서 `java_sdk.enabled: false` 설정
2. REST API 또는 SSH 방식 사용
3. 로컬 Export 파일 파싱 사용

### JVM 시작 실패 시
1. Java 경로 확인: `config.yaml`의 `java_home` 설정 확인
2. JAR 파일 경로 확인: `client_path` 설정 확인
3. 환경 변수 설정: `JAVA_HOME` 환경 변수 설정

## 참고 자료

- [JPype 문서](https://jpype.readthedocs.io/)
- [IBM DataStage 문서](https://www.ibm.com/docs/en/infosphere-datastage)

