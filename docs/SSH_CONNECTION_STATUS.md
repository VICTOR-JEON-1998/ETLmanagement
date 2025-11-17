# SSH 연결 상태

## 현재 상태

SSH 연결이 **인증 실패**로 실패하고 있습니다.

- 호스트: `10.100.20.70`
- 사용자: `etl_admin`
- 비밀번호: `etletl` (길이: 6자)
- 상태: ❌ 인증 실패

## 해결 방법

### 1. MobaXterm으로 직접 연결 테스트

MobaXterm에서 다음 정보로 연결해보세요:
```
Host: 10.100.20.70
Username: etl_admin
Password: etletl
```

연결이 성공하면:
- 비밀번호가 정확한지 확인
- 사용자 이름이 정확한지 확인

### 2. 다른 계정 시도

다른 계정 정보가 있다면 알려주세요:
- `dsadm` 계정
- 다른 관리자 계정

### 3. 현재 사용 가능한 방법

SSH 연결이 실패해도 **로컬 DSX 파일**을 사용하여 Job 목록을 조회할 수 있습니다:

```bash
# Export 파일에서 Job 목록 조회
python cli/main.py list-jobs --project BIDW_ADM
```

## SSH 연결이 성공하면

- ✅ Export **불필요**
- ✅ 실시간 Job 목록 조회
- ✅ 서버에서 직접 Job 정보 가져오기
- ✅ Job 정의 실시간 조회

## 다음 단계

1. MobaXterm으로 SSH 연결 확인
2. 정확한 계정 정보 확인
3. 연결 성공 시 자동으로 SSH 방식 사용

