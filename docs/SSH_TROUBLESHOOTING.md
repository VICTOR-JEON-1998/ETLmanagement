# SSH 연결 문제 해결 가이드

## 현재 문제

SSH 연결이 실패하여 로컬 DSX 파일을 사용하고 있습니다.

## 해결 방법

### 1. 비밀번호 확인

`config/.env` 파일에서 SSH 비밀번호를 확인하세요:

```env
SSH_PASSWORD=Fila2023!
```

### 2. SSH 연결 테스트

MobaXterm이나 다른 SSH 클라이언트로 직접 연결 테스트:

```bash
ssh useradmin@10.100.20.71
```

### 3. DataStage 계정 사용

`dsadm` 계정을 사용할 수도 있습니다:

```yaml
ssh:
  username: "dsadm"
  password: "${DS_PASSWORD}"  # .env에서 DS_PASSWORD 확인
```

### 4. SSH 키 인증 사용 (선택사항)

비밀번호 대신 SSH 키를 사용할 수 있습니다.

## SSH 연결이 성공하면

- ✅ Export **불필요**
- ✅ 실시간 Job 목록 조회
- ✅ 서버에서 직접 Job 정보 가져오기

## 임시 해결책: Export 사용

SSH 연결이 해결될 때까지 로컬 Export 파일을 사용할 수 있습니다:

1. DataStage에서 Job 전체 Export
2. Export 파일을 `C:\Users\D001_2240052\Desktop\DBA-DE\ETL job files`에 저장
3. 시스템이 자동으로 인식

**단, Export는 한 번만 하면 됩니다.** 새로운 Job이 추가되면 다시 Export하거나 SSH 연결을 개선하세요.

