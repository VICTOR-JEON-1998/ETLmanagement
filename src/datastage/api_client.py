"""IBM DataStage REST API 클라이언트"""

import requests
from typing import Dict, Any, Optional, List
from requests.auth import HTTPBasicAuth
from urllib3.exceptions import InsecureRequestWarning

from src.core.config import get_config
from src.core.logger import get_logger

# SSL 경고 비활성화 (자체 서명 인증서 사용 시)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logger = get_logger(__name__)

# SSH 클라이언트는 필요시에만 import (순환 참조 방지)
try:
    from src.datastage.ssh_client import DataStageSSHClient
    SSH_AVAILABLE = True
except ImportError:
    SSH_AVAILABLE = False
    logger.warning("SSH 클라이언트를 사용할 수 없습니다. paramiko가 설치되어 있는지 확인하세요.")

# Java SDK 클라이언트는 필요시에만 import
try:
    from src.datastage.java_sdk_client import DataStageJavaSDKClient
    JAVA_SDK_AVAILABLE = True
except ImportError:
    JAVA_SDK_AVAILABLE = False
    logger.debug("Java SDK 클라이언트를 사용할 수 없습니다. JPype1이 설치되어 있는지 확인하세요.")


class DataStageAPIClient:
    """IBM DataStage REST API 클라이언트"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        DataStage API 클라이언트 초기화
        
        Args:
            config: DataStage 설정 딕셔너리 (None이면 config에서 로드)
        """
        if config is None:
            config = get_config().get_datastage_config()
        
        self.server_host = config.get("server_host")
        self.server_port = config.get("server_port", 9446)
        self.username = config.get("username")
        self.password = config.get("password")
        self.api_base_url = config.get("api_base_url", f"https://{self.server_host}:{self.server_port}/ibm/iis/api")
        
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)
        self.session.verify = False  # SSL 검증 비활성화 (필요시 수정)
        
        self._token: Optional[str] = None
    
    def _get_headers(self) -> Dict[str, str]:
        """요청 헤더 생성"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers
    
    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        raise_on_error: bool = True
    ) -> requests.Response:
        """
        HTTP 요청 실행
        
        Args:
            method: HTTP 메서드 (GET, POST, PUT, DELETE)
            endpoint: API 엔드포인트
            data: 요청 본문 데이터
            params: 쿼리 파라미터
            raise_on_error: 오류 발생 시 예외 발생 여부
        
        Returns:
            응답 객체
        """
        url = f"{self.api_base_url}/{endpoint.lstrip('/')}"
        headers = self._get_headers()
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                params=params,
                timeout=30
            )
            if raise_on_error:
                response.raise_for_status()
            logger.debug(f"API 요청 성공: {method} {url} (Status: {response.status_code})")
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"API 요청 실패: {method} {url} - {e}")
            if raise_on_error:
                raise
            return response  # 오류가 발생해도 응답 객체 반환
    
    def authenticate(self) -> bool:
        """
        인증 및 토큰 획득
        
        Returns:
            인증 성공 여부
        """
        try:
            # DataStage REST API 인증 엔드포인트 (버전에 따라 다를 수 있음)
            endpoint = "v1/auth/login"  # 또는 다른 엔드포인트
            response = self._request("POST", endpoint, data={
                "username": self.username,
                "password": self.password
            })
            
            if response.status_code == 200:
                result = response.json()
                self._token = result.get("token")
                logger.info("DataStage 인증 성공")
                return True
            else:
                logger.warning("DataStage 인증 실패: REST API가 지원되지 않을 수 있습니다")
                return False
        except Exception as e:
            logger.warning(f"DataStage REST API 인증 실패: {e}. 기본 인증(Basic Auth) 사용")
            return False
    
    def get_projects(self) -> List[Dict[str, Any]]:
        """
        프로젝트 목록 조회
        
        Returns:
            프로젝트 정보 리스트
        """
        try:
            response = self._request("GET", "v1/projects")
            return response.json().get("projects", [])
        except Exception as e:
            logger.error(f"프로젝트 목록 조회 실패: {e}")
            return []
    
    def get_jobs(self, project_name: str) -> List[Dict[str, Any]]:
        """
        Job 목록 조회
        
        Args:
            project_name: 프로젝트 이름
        
        Returns:
            Job 정보 리스트
        """
        # REST API 시도
        try:
            endpoint = f"v1/projects/{project_name}/jobs"
            response = self._request("GET", endpoint, raise_on_error=False)
            if response.status_code < 400:
                return response.json().get("jobs", [])
        except Exception as e:
            logger.debug(f"REST API로 Job 목록 조회 실패: {e}")
        
        # REST API 실패 시 Designer 클라이언트 사용 (로컬 툴 직접 연동)
        try:
            from src.datastage.designer_client import DataStageDesignerClient
            designer_client = DataStageDesignerClient()
            designer_jobs = designer_client.get_jobs(project_name)
            if designer_jobs:
                logger.info(f"Designer 클라이언트로 {len(designer_jobs)}개 Job 발견")
                return designer_jobs
        except Exception as e:
            logger.debug(f"Designer 클라이언트로 Job 목록 조회 실패: {e}")
        
        # Designer 클라이언트 실패 시 Java SDK 사용 (활성화된 경우만)
        if JAVA_SDK_AVAILABLE:
            try:
                config = get_config().get_datastage_config()
                java_sdk_config = config.get("java_sdk", {})
                if java_sdk_config.get("enabled", False):
                    java_sdk_client = DataStageJavaSDKClient()
                    if java_sdk_client.connect():
                        jobs = java_sdk_client.get_jobs(project_name)
                        java_sdk_client.close()
                        if jobs:
                            logger.info(f"Java SDK로 Job 목록 조회 성공: {len(jobs)}개")
                            return jobs
            except Exception as e:
                logger.debug(f"Java SDK로 Job 목록 조회 실패: {e}")
        
        # Java SDK 실패 시 SSH 사용
        if SSH_AVAILABLE:
            try:
                from src.datastage.ssh_client import DataStageSSHClient
                ssh_client = DataStageSSHClient()
                if ssh_client.connect():
                    jobs = ssh_client.get_jobs(project_name)
                    ssh_client.close()
                    if jobs:
                        return jobs
            except Exception as e:
                logger.debug(f"SSH로 Job 목록 조회 실패: {e}")
        
        # SSH 실패 시 로컬 DSX 파일 사용
        try:
            from src.datastage.local_client import DataStageLocalClient
            local_client = DataStageLocalClient()
            local_jobs = local_client.get_jobs(project_name)
            if local_jobs:
                logger.info(f"로컬 DSX 파일에서 {len(local_jobs)}개 Job 발견")
                return local_jobs
        except Exception as e:
            logger.debug(f"로컬 DSX 파일 조회 실패: {e}")
        
        return []
    
    def get_job_definition(self, project_name: str, job_name: str) -> Optional[Dict[str, Any]]:
        """
        Job 정의 조회
        
        Args:
            project_name: 프로젝트 이름
            job_name: Job 이름
        
        Returns:
            Job 정의 딕셔너리 또는 XML 문자열
        """
        # REST API 시도
        try:
            endpoint = f"v1/projects/{project_name}/jobs/{job_name}"
            response = self._request("GET", endpoint, raise_on_error=False)
            if response.status_code < 400:
                return response.json()
        except Exception as e:
            logger.debug(f"REST API로 Job 정의 조회 실패: {e}")
        
        # REST API 실패 시 Java SDK 사용 (활성화된 경우만)
        if JAVA_SDK_AVAILABLE:
            try:
                config = get_config().get_datastage_config()
                java_sdk_config = config.get("java_sdk", {})
                if java_sdk_config.get("enabled", False):
                    java_sdk_client = DataStageJavaSDKClient()
                    if java_sdk_client.connect():
                        job_def = java_sdk_client.get_job_definition(project_name, job_name)
                        java_sdk_client.close()
                        if job_def:
                            logger.info(f"Java SDK로 Job 정의 조회 성공: {project_name}/{job_name}")
                            return job_def
            except Exception as e:
                logger.debug(f"Java SDK로 Job 정의 조회 실패: {e}")
        
        # Java SDK 실패 시 SSH 사용
        if SSH_AVAILABLE:
            try:
                from src.datastage.ssh_client import DataStageSSHClient
                ssh_client = DataStageSSHClient()
                if ssh_client.connect():
                    # Job 파일 찾기
                    job_files = ssh_client.get_job_files(project_name)
                    for job_file in job_files:
                        from pathlib import Path
                        if job_name in job_file or Path(job_file).stem == job_name:
                            content = ssh_client.read_job_file(job_file)
                            ssh_client.close()
                            if content:
                                return {
                                    "name": job_name,
                                    "project": project_name,
                                    "file_path": job_file,
                                    "content": content,
                                    "format": "dsx" if job_file.endswith(".dsx") else "isx"
                                }
                    ssh_client.close()
            except Exception as e:
                logger.debug(f"SSH로 Job 정의 조회 실패: {e}")
        
        # SSH 실패 시 로컬 DSX 파일 사용
        try:
            from src.datastage.local_client import DataStageLocalClient
            local_client = DataStageLocalClient()
            job_def = local_client.get_job_definition(job_name, project_name)
            if job_def:
                logger.info(f"로컬 DSX 파일에서 Job 정의 조회 성공: {job_name}")
                return job_def
        except Exception as e:
            logger.debug(f"로컬 DSX 파일에서 Job 정의 조회 실패: {e}")
        
        return None
    
    def update_job_definition(
        self,
        project_name: str,
        job_name: str,
        job_definition: Dict[str, Any]
    ) -> bool:
        """
        Job 정의 업데이트
        
        Args:
            project_name: 프로젝트 이름
            job_name: Job 이름
            job_definition: 업데이트할 Job 정의
        
        Returns:
            업데이트 성공 여부
        """
        try:
            endpoint = f"v1/projects/{project_name}/jobs/{job_name}"
            response = self._request("PUT", endpoint, data=job_definition)
            logger.info(f"Job 정의 업데이트 성공: {project_name}/{job_name}")
            return True
        except Exception as e:
            logger.error(f"Job 정의 업데이트 실패: {e}")
            return False
    
    def search_jobs_by_table(
        self,
        project_name: str,
        table_name: str,
        schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        특정 테이블을 사용하는 Job 검색
        
        Args:
            project_name: 프로젝트 이름
            table_name: 테이블 이름
            schema: 스키마 이름 (선택)
        
        Returns:
            Job 정보 리스트
        """
        # 전체 Job 목록 조회 후 필터링
        # (실제 API가 검색 기능을 제공하면 해당 기능 사용)
        all_jobs = self.get_jobs(project_name)
        matching_jobs = []
        
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        for job in all_jobs:
            job_def = self.get_job_definition(project_name, job.get("name"))
            if job_def and self._job_uses_table(job_def, full_table_name):
                matching_jobs.append(job)
        
        return matching_jobs
    
    def _job_uses_table(self, job_definition: Dict[str, Any], table_name: str) -> bool:
        """
        Job이 특정 테이블을 사용하는지 확인
        
        Args:
            job_definition: Job 정의
            table_name: 테이블 이름
        
        Returns:
            사용 여부
        """
        # Job 정의를 문자열로 변환하여 테이블 이름 검색
        job_str = str(job_definition).lower()
        table_lower = table_name.lower()
        return table_lower in job_str
    
    def test_connection(self) -> Dict[str, Any]:
        """
        연결 테스트 (상세 정보 반환)
        
        Returns:
            연결 테스트 결과 딕셔너리
        """
        result = {
            "success": False,
            "method": None,
            "endpoint": None,
            "status_code": None,
            "error": None,
            "details": []
        }
        
        # 여러 엔드포인트 시도
        test_endpoints = [
            ("v1/health", "GET"),
            ("v1/projects", "GET"),
            ("ibm/iis/api/v1/health", "GET"),
            ("ibm/iis/api/v1/projects", "GET"),
            ("api/v1/health", "GET"),
            ("api/v1/projects", "GET"),
            ("", "GET"),  # 루트 경로
        ]
        
        # 여러 API 베이스 URL 시도
        base_urls = [
            self.api_base_url,
            f"https://{self.server_host}:{self.server_port}/ibm/iis/api",
            f"https://{self.server_host}:{self.server_port}/ibm/iis/rest",
            f"https://{self.server_host}:{self.server_port}/ibm/iis",
            f"https://{self.server_host}:{self.server_port}/api",
        ]
        
        for base_url in base_urls:
            for endpoint, method in test_endpoints:
                try:
                    url = f"{base_url}/{endpoint.lstrip('/')}" if endpoint else base_url
                    headers = self._get_headers()
                    
                    response = self.session.request(
                        method=method,
                        url=url,
                        headers=headers,
                        timeout=10,
                        verify=False
                    )
                    
                    result["details"].append({
                        "url": url,
                        "method": method,
                        "status_code": response.status_code,
                        "success": response.status_code < 400
                    })
                    
                    # 성공한 경우
                    if response.status_code < 400:
                        result["success"] = True
                        result["method"] = method
                        result["endpoint"] = endpoint
                        result["status_code"] = response.status_code
                        # 성공한 URL로 업데이트
                        self.api_base_url = base_url
                        logger.info(f"DataStage 연결 성공: {url}")
                        return result
                    
                except requests.exceptions.SSLError as e:
                    result["details"].append({
                        "url": url,
                        "method": method,
                        "error": f"SSL 오류: {e}",
                        "success": False
                    })
                except requests.exceptions.ConnectionError as e:
                    result["details"].append({
                        "url": url,
                        "method": method,
                        "error": f"연결 오류: {e}",
                        "success": False
                    })
                except requests.exceptions.Timeout as e:
                    result["details"].append({
                        "url": url,
                        "method": method,
                        "error": f"타임아웃: {e}",
                        "success": False
                    })
                except Exception as e:
                    result["details"].append({
                        "url": url,
                        "method": method,
                        "error": str(e),
                        "success": False
                    })
        
        # REST API 실패 시 Java SDK 시도 (활성화된 경우만)
        if not result["success"] and JAVA_SDK_AVAILABLE:
            try:
                config = get_config().get_datastage_config()
                java_sdk_config = config.get("java_sdk", {})
                if java_sdk_config.get("enabled", False):
                    logger.info("REST API 실패, Java SDK 접근 방법 시도...")
                    java_sdk_client = DataStageJavaSDKClient()
                    java_result = java_sdk_client.test_connection()
                    if java_result["success"]:
                        result["success"] = True
                        result["method"] = "java_sdk"
                        result["java_sdk_info"] = java_result
                        logger.info("Java SDK를 통한 DataStage 연결 성공")
                        return result
            except Exception as e:
                logger.debug(f"Java SDK 연결 테스트 실패: {e}")
        
        # Java SDK 실패 시 SSH 시도
        if not result["success"] and SSH_AVAILABLE:
            logger.info("Java SDK 실패, SSH 접근 방법 시도...")
            ssh_result = self._test_ssh_connection()
            if ssh_result["success"]:
                result["success"] = True
                result["method"] = "ssh"
                result["ssh_info"] = ssh_result
                logger.info("SSH를 통한 DataStage 연결 성공")
                return result
        
        # 모든 시도 실패
        if not result["success"]:
            result["error"] = "모든 엔드포인트 시도 실패. REST API가 지원되지 않거나 설정이 잘못되었을 수 있습니다."
            logger.warning(result["error"])
        return result
    
    def _test_ssh_connection(self) -> Dict[str, Any]:
        """SSH 연결 테스트"""
        try:
            ssh_client = DataStageSSHClient()
            if ssh_client.connect():
                # DataStage 정보 조회
                info = ssh_client.get_datastage_info()
                ssh_client.close()
                
                return {
                    "success": True,
                    "datastage_path": info.get("datastage_path"),
                    "projects": info.get("projects", []),
                    "version": info.get("version"),
                    "status": info.get("status")
                }
        except Exception as e:
            logger.error(f"SSH 연결 테스트 실패: {e}")
        
        return {"success": False}
    
    def test_connection_simple(self) -> bool:
        """
        간단한 연결 테스트 (기존 호환성 유지)
        
        Returns:
            연결 성공 여부
        """
        result = self.test_connection()
        return result["success"]

