"""SSH를 통한 DataStage 서버 접근 모듈"""

import paramiko
from typing import Dict, Any, Optional, List
from pathlib import Path

from src.core.config import get_config
from src.core.logger import get_logger

logger = get_logger(__name__)


class DataStageSSHClient:
    """SSH를 통한 DataStage 서버 접근 클래스"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        SSH 클라이언트 초기화
        
        Args:
            config: SSH 설정 딕셔너리 (None이면 config에서 로드)
        """
        if config is None:
            ds_config = get_config().get_datastage_config()
            ssh_config = ds_config.get("ssh", {})
        else:
            ssh_config = config.get("ssh", {})
        
        # SSH 접근 정보
        self.ssh_host = ssh_config.get("host", "10.100.20.70")
        self.ssh_port = ssh_config.get("port", 22)
        self.ssh_username = ssh_config.get("username", "etl_admin")
        # 비밀번호 처리
        ssh_password = ssh_config.get("password", "etletl")
        import os
        # 환경 변수 치환 문자열인 경우
        if isinstance(ssh_password, str) and ssh_password.startswith("${") and ssh_password.endswith("}"):
            # 환경 변수 치환이 안 된 경우 기본값 사용
            env_var = ssh_password[2:-1]
            env_value = os.getenv(env_var)
            if env_value:
                self.ssh_password = env_value
                logger.debug(f"SSH 비밀번호 로드: 환경 변수 {env_var}에서 가져옴")
            else:
                # 환경 변수가 없으면 기본값 사용
                self.ssh_password = "etletl"
                logger.debug(f"SSH 비밀번호 로드: 환경 변수 {env_var} 없음, 기본값 사용")
        else:
            # 직접 설정된 비밀번호 사용
            self.ssh_password = ssh_password
            logger.debug(f"SSH 비밀번호 로드: 설정 파일에서 직접 가져옴 (길이: {len(self.ssh_password)})")
        logger.debug(f"SSH 연결 정보: {self.ssh_username}@{self.ssh_host}:{self.ssh_port}")
        
        self.client = None
    
    def connect(self) -> bool:
        """SSH 연결"""
        try:
            logger.debug(f"SSH 연결 시도: {self.ssh_username}@{self.ssh_host}:{self.ssh_port}")
            logger.debug(f"비밀번호 길이: {len(self.ssh_password) if self.ssh_password else 0}")
            
            # 로컬 IP 주소 확인 (디버깅용)
            try:
                import socket
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(('8.8.8.8', 80))
                local_ip = s.getsockname()[0]
                s.close()
                logger.debug(f"출발지 IP 주소: {local_ip}")
            except:
                pass
            
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # 다양한 SSH 알고리즘 시도 (호환성 개선)
            # 일부 서버는 특정 알고리즘만 허용할 수 있음
            try:
                self.client.connect(
                    hostname=self.ssh_host,
                    port=self.ssh_port,
                    username=self.ssh_username,
                    password=self.ssh_password,
                    timeout=10,
                    look_for_keys=False,  # SSH 키 검색 비활성화
                    allow_agent=False,  # SSH agent 비활성화
                    # 추가 옵션
                    compress=False,
                    disabled_algorithms={'pubkeys': []}  # 모든 공개키 알고리즘 허용
                )
            except paramiko.ssh_exception.SSHException as e:
                # 알고리즘 문제일 수 있으므로 다른 방법 시도
                logger.debug(f"첫 번째 연결 시도 실패: {e}")
                # 기본 설정으로 재시도
                self.client = paramiko.SSHClient()
                self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self.client.connect(
                    hostname=self.ssh_host,
                    port=self.ssh_port,
                    username=self.ssh_username,
                    password=self.ssh_password,
                    timeout=15,  # 타임아웃 증가
                    look_for_keys=False,
                    allow_agent=False
                )
            
            logger.info(f"SSH 연결 성공: {self.ssh_host}")
            return True
        except paramiko.AuthenticationException as e:
            logger.error(f"SSH 인증 실패: {e}")
            logger.error(f"연결 정보: {self.ssh_username}@{self.ssh_host}:{self.ssh_port}")
            logger.error(f"비밀번호 길이: {len(self.ssh_password) if self.ssh_password else 0}")
            logger.error("가능한 원인:")
            logger.error("  1. 비밀번호가 잘못되었습니다")
            logger.error("  2. 사용자 이름이 잘못되었습니다")
            logger.error("  3. 서버에서 이 IP의 접근을 허용하지 않습니다")
            logger.error("  4. 계정이 비활성화되었습니다")
            return False
        except Exception as e:
            logger.error(f"SSH 연결 실패: {e}")
            logger.error(f"연결 정보: {self.ssh_username}@{self.ssh_host}:{self.ssh_port}, 비밀번호 설정 여부: {bool(self.ssh_password)}")
            import traceback
            logger.debug(traceback.format_exc())
            return False
    
    def execute_command(self, command: str, use_sudo: bool = False) -> Dict[str, Any]:
        """
        원격 명령 실행
        
        Args:
            command: 실행할 명령어
            use_sudo: sudo -i로 root 전환 후 실행 여부
        
        Returns:
            실행 결과 딕셔너리 (stdout, stderr, exit_status)
        """
        if not self.client:
            if not self.connect():
                return {"stdout": "", "stderr": "SSH 연결 실패", "exit_status": -1}
        
        try:
            # sudo -i로 root 전환 후 명령 실행
            if use_sudo:
                # sudo -i는 대화형 세션이므로, 명령을 직접 실행하는 방식 사용
                # 명령어에 작은따옴표가 포함될 수 있으므로 이스케이프 처리
                # sudo -i -c 'command' 형식 사용
                escaped_command = command.replace("'", "'\"'\"'")
                full_command = f"sudo -i bash -c '{escaped_command}'"
            else:
                full_command = command
            
            stdin, stdout, stderr = self.client.exec_command(full_command)
            exit_status = stdout.channel.recv_exit_status()
            
            return {
                "stdout": stdout.read().decode('utf-8', errors='ignore'),
                "stderr": stderr.read().decode('utf-8', errors='ignore'),
                "exit_status": exit_status
            }
        except Exception as e:
            logger.error(f"명령 실행 실패: {e}")
            return {"stdout": "", "stderr": str(e), "exit_status": -1}
    
    def find_datastage_path(self) -> Optional[str]:
        """DataStage 설치 경로 찾기"""
        commands = [
            "which dsengine 2>/dev/null",
            "find /opt -name dsengine 2>/dev/null | head -1",
            "find /usr -name dsengine 2>/dev/null | head -1",
            "ls -d /opt/IBM/InformationServer* 2>/dev/null | head -1",
            "ls -d /opt/IBM/InformationServer/Server 2>/dev/null",
        ]
        
        for cmd in commands:
            result = self.execute_command(cmd)
            if result["exit_status"] == 0 and result["stdout"].strip():
                path = result["stdout"].strip()
                logger.info(f"DataStage 경로 발견: {path}")
                return path
        
        return None
    
    def get_datastage_projects(self) -> List[str]:
        """DataStage 프로젝트 목록 조회"""
        projects = []
        
        # 프로젝트 디렉토리 찾기
        project_dirs = [
            "/opt/IBM/InformationServer/Server/Projects",
            "/opt/IBM/InformationServer/Projects",
            "/home/dsadm/Projects",
        ]
        
        for project_dir in project_dirs:
            # 디렉토리 존재 확인
            check_result = self.execute_command(f"test -d '{project_dir}' && echo 'exists' || echo 'not exists'")
            if "exists" not in check_result["stdout"]:
                continue
            
            # 프로젝트 목록 조회 (디렉토리만)
            result = self.execute_command(f"ls -1d '{project_dir}'/* 2>/dev/null")
            if result["exit_status"] == 0:
                for line in result["stdout"].strip().split('\n'):
                    if line.strip():
                        item_path = line.strip()
                        # 디렉토리인지 확인
                        dir_check = self.execute_command(f"test -d '{item_path}' && echo 'dir' || echo 'file'")
                        if "dir" in dir_check["stdout"]:
                            project_name = Path(item_path).name
                            if project_name and project_name not in projects:
                                projects.append(project_name)
        
        # 기본 프로젝트 추가 (이미지에서 확인된 프로젝트)
        if "BIDW_ADM" not in projects:
            projects.append("BIDW_ADM")
        
        return list(set(projects))
    
    def get_job_files(self, project_name: str) -> List[str]:
        """프로젝트의 Job 파일 목록 조회"""
        # 먼저 프로젝트 경로 찾기
        project_paths = []
        
        # 일반적인 DataStage 프로젝트 경로들
        search_paths = [
            f"/opt/IBM/InformationServer/Server/Projects/{project_name}",
            f"/home/dsadm/Projects/{project_name}",
            f"/opt/IBM/InformationServer/Projects/{project_name}",
        ]
        
        # 프로젝트 디렉토리 존재 확인
        for path in search_paths:
            # 일반 접근 시도
            result = self.execute_command(f"test -d '{path}' && echo 'exists' || echo 'not exists'")
            if result["exit_status"] == 0 and "exists" in result["stdout"]:
                project_paths.append(path)
            # sudo -i로 root 전환 후 접근 시도
            else:
                result = self.execute_command(f"test -d '{path}' && echo 'exists' || echo 'not exists'", use_sudo=True)
                if result["exit_status"] == 0 and "exists" in result["stdout"]:
                    project_paths.append(path)
        
        # 프로젝트 경로를 찾지 못한 경우, 전체 검색
        if not project_paths:
            # DataStage 설치 경로 찾기
            ds_path = self.find_datastage_path()
            if ds_path:
                # Server 디렉토리 찾기
                server_paths = [
                    "/opt/IBM/InformationServer/Server",
                    "/opt/IBM/InformationServer",
                    "/home/dsadm",
                ]
                for base_path in server_paths:
                    result = self.execute_command(f"find {base_path} -type d -name '{project_name}' 2>/dev/null | head -3")
                    if result["exit_status"] == 0 and result["stdout"].strip():
                        for line in result["stdout"].strip().split('\n'):
                            if line.strip():
                                project_paths.append(line.strip())
        
        # Job 파일 찾기 (.dsx, .isx, .xml 등)
        job_files = []
        extensions = ['*.dsx', '*.isx', '*.xml']
        
        for project_path in project_paths:
            for ext in extensions:
                cmd = f"find '{project_path}' -name '{ext}' -type f 2>/dev/null"
                # 일반 접근 시도
                result = self.execute_command(cmd)
                if result["exit_status"] == 0:
                    for line in result["stdout"].strip().split('\n'):
                        if line.strip() and project_name.lower() in line.lower():
                            job_files.append(line.strip())
                # sudo -i로 root 전환 후 접근 시도
                if not result["stdout"].strip():
                    result = self.execute_command(cmd, use_sudo=True)
                    if result["exit_status"] == 0:
                        for line in result["stdout"].strip().split('\n'):
                            if line.strip() and project_name.lower() in line.lower():
                                job_files.append(line.strip())
        
        # 중복 제거
        return list(set(job_files))
    
    def get_jobs(self, project_name: str) -> List[Dict[str, Any]]:
        """
        프로젝트의 Job 목록 조회
        
        Args:
            project_name: 프로젝트 이름
        
        Returns:
            Job 정보 리스트
        """
        jobs = []
        
        # 방법 1: DataStage Repository에서 직접 조회 (dsjob 명령어)
        # dsjob 명령어는 DataStage 환경이 설정되어 있어야 함
        # 여러 dsenv 경로 시도
        dsenv_paths = [
            "/opt/IBM/InformationServer/Server/DSEngine/dsenv",
            "/opt/IBM/InformationServer/DSEngine/dsenv",
        ]
        
        for dsenv_path in dsenv_paths:
            # dsenv 파일 존재 확인
            check_dsenv = self.execute_command(f"test -f '{dsenv_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
            if "EXISTS" not in check_dsenv["stdout"]:
                continue
            
            # dsjob 명령어 실행
            dsjob_cmd = f"source '{dsenv_path}' 2>/dev/null; dsjob -listjobs {project_name} 2>&1"
            result = self.execute_command(dsjob_cmd)
            
            if result["exit_status"] == 0 and result["stdout"].strip():
                # dsjob 출력 파싱
                for line in result["stdout"].strip().split('\n'):
                    if line.strip() and not line.strip().startswith('#'):
                        parts = line.strip().split()
                        if len(parts) >= 1:
                            job_name = parts[0].strip()
                            if job_name and job_name.lower() != 'job':
                                jobs.append({
                                    "name": job_name,
                                    "file_path": None,
                                    "project": project_name,
                                    "source": "dsjob"
                                })
                if jobs:
                    logger.info(f"dsjob으로 {len(jobs)}개 Job 발견")
                    break
        
        # 방법 2: 프로젝트 디렉토리에서 직접 찾기 (우선순위 높음)
        search_paths = [
            f"/opt/IBM/InformationServer/Server/Projects/{project_name}",
            f"/home/dsadm/Projects/{project_name}",
            f"/opt/IBM/InformationServer/Projects/{project_name}",
        ]
        
        # 프로젝트 디렉토리 찾기
        found_project_path = None
        for path in search_paths:
            # 일반 접근 시도
            result = self.execute_command(f"test -d '{path}' && echo 'exists' || echo 'not exists'")
            if result["exit_status"] == 0 and "exists" in result["stdout"]:
                found_project_path = path
                break
            # sudo -i로 root 전환 후 접근 시도
            result = self.execute_command(f"test -d '{path}' && echo 'exists' || echo 'not exists'", use_sudo=True)
            if result["exit_status"] == 0 and "exists" in result["stdout"]:
                found_project_path = path
                break
        
        # 프로젝트 경로를 찾지 못한 경우, 전체 검색
        if not found_project_path:
            for base_path in ["/opt/IBM/InformationServer/Server/Projects", "/opt/IBM/InformationServer/Projects", "/home/dsadm"]:
                result = self.execute_command(f"find {base_path} -type d -name '{project_name}' 2>/dev/null | head -1", use_sudo=True)
                if result["exit_status"] == 0 and result["stdout"].strip():
                    found_project_path = result["stdout"].strip()
                    break
        
        if found_project_path:
            # Jobs 디렉토리에서 직접 찾기 (가장 확실한 방법)
            jobs_path = f"{found_project_path}/Jobs"
            check_jobs_dir = self.execute_command(f"test -d '{jobs_path}' && echo 'EXISTS' || echo 'NOT_EXISTS'")
            
            if "EXISTS" in check_jobs_dir["stdout"]:
                # Jobs 디렉토리 내용 확인
                commands = [
                    (f"ls -1 '{jobs_path}' 2>/dev/null | head -200", False),
                    (f"find '{jobs_path}' -maxdepth 1 -type d 2>/dev/null | head -200", False),
                    (f"ls -1d '{jobs_path}'/* 2>/dev/null | head -200", False),
                ]
                
                for cmd, use_sudo in commands:
                    result = self.execute_command(cmd, use_sudo=use_sudo)
                    if result["exit_status"] == 0 and result["stdout"].strip():
                        for line in result["stdout"].strip().split('\n'):
                            if line.strip():
                                item_path = line.strip()
                                item_name = Path(item_path).name
                                
                                # Job 이름으로 보이는 항목 추가
                                if item_name and item_name not in ['.', '..']:
                                    jobs.append({
                                        "name": item_name,
                                        "file_path": item_path,
                                        "project": project_name,
                                        "source": "jobs_directory"
                                    })
                        if jobs:
                            logger.info(f"Jobs 디렉토리에서 {len(jobs)}개 Job 발견")
                            break
            
            # Jobs 디렉토리를 찾지 못한 경우, 프로젝트 루트에서 찾기
            if not jobs:
                commands = [
                    (f"ls -1d '{found_project_path}'/* 2>/dev/null | head -100", False),
                    (f"ls -1d '{found_project_path}'/* 2>/dev/null | head -100", True),  # sudo -i 사용
                ]
                
                for cmd, use_sudo in commands:
                    result = self.execute_command(cmd, use_sudo=use_sudo)
                    if result["exit_status"] == 0 and result["stdout"].strip():
                        for line in result["stdout"].strip().split('\n'):
                            if line.strip():
                                item_path = line.strip()
                                item_name = Path(item_path).name
                                # Job 디렉토리인지 확인
                                check_dir = self.execute_command(f"test -d '{item_path}' && echo 'dir' || echo 'file'", use_sudo=use_sudo)
                                is_dir = check_dir["stdout"].strip() == "dir"
                                
                                # Job으로 보이는 항목 추가
                                if item_name and item_name not in ['.', '..']:
                                    jobs.append({
                                        "name": item_name,
                                        "file_path": item_path,
                                        "project": project_name,
                                        "source": "directory" if is_dir else "file"
                                    })
                        if jobs:
                            break
        
        # 방법 3: Job 파일에서 찾기 (.dsx, .isx 등)
        if not jobs:
            job_files = self.get_job_files(project_name)
            for job_file in job_files:
                job_name = Path(job_file).stem
                if job_name:
                    jobs.append({
                        "name": job_name,
                        "file_path": job_file,
                        "project": project_name,
                        "source": "file"
                    })
        
        # 중복 제거 (이름 기준)
        seen = set()
        unique_jobs = []
        for job in jobs:
            job_name = job.get("name", "").lower()
            if job_name and job_name not in seen:
                seen.add(job_name)
                unique_jobs.append(job)
        
        return unique_jobs
    
    def read_job_file(self, file_path: str) -> Optional[str]:
        """Job 파일 읽기"""
        result = self.execute_command(f"cat '{file_path}'")
        if result["exit_status"] == 0:
            return result["stdout"]
        return None
    
    def search_jobs_by_table(self, project_name: str, table_name: str) -> List[str]:
        """테이블을 사용하는 Job 찾기"""
        job_files = self.get_job_files(project_name)
        matching_jobs = []
        
        for job_file in job_files:
            content = self.read_job_file(job_file)
            if content and table_name.lower() in content.lower():
                matching_jobs.append(job_file)
        
        return matching_jobs
    
    def get_datastage_info(self) -> Dict[str, Any]:
        """DataStage 서버 정보 조회"""
        info = {
            "datastage_path": None,
            "projects": [],
            "version": None,
            "status": "unknown"
        }
        
        # DataStage 경로 찾기
        info["datastage_path"] = self.find_datastage_path()
        
        # 프로젝트 목록
        info["projects"] = self.get_datastage_projects()
        
        # 버전 정보
        if info["datastage_path"]:
            version_cmd = f"{info['datastage_path']}/bin/dsengine -version 2>/dev/null || echo 'unknown'"
            result = self.execute_command(version_cmd)
            if result["exit_status"] == 0:
                info["version"] = result["stdout"].strip()
        
        # 서비스 상태
        status_cmd = "systemctl status dsrpc 2>/dev/null | grep Active || ps aux | grep dsrpc | grep -v grep || echo 'unknown'"
        result = self.execute_command(status_cmd)
        if result["exit_status"] == 0:
            info["status"] = result["stdout"].strip()[:100]  # 처음 100자만
        
        return info
    
    def close(self):
        """SSH 연결 종료"""
        if self.client:
            self.client.close()
            logger.info("SSH 연결 종료")
    
    def __enter__(self):
        """컨텍스트 매니저 진입"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        self.close()

