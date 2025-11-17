"""DataStage Designer 클라이언트 (로컬 툴 직접 연동)"""

import subprocess
import os
from typing import Dict, Any, Optional, List
from pathlib import Path
import xml.etree.ElementTree as ET
import re

from src.core.config import get_config
from src.core.logger import get_logger

logger = get_logger(__name__)


class DataStageDesignerClient:
    """DataStage Designer 클라이언트 (로컬 툴 직접 연동)"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Designer 클라이언트 초기화
        
        Args:
            config: DataStage 설정 딕셔너리 (None이면 config에서 로드)
        """
        if config is None:
            config = get_config().get_datastage_config()
        
        # Classic 클라이언트 경로
        self.classic_path = Path("C:/IBM/InformationServer/Clients/Classic")
        self.dsjob_exe = self.classic_path / "dsjob.exe"
        self.dsexport_exe = self.classic_path / "dsexport.exe"
        self.dsimport_exe = self.classic_path / "dsimport.exe"
        self.dsadmin_exe = self.classic_path / "dsadmin.exe"
        
        # 서버 정보
        self.server_host = config.get("server_host", "CM-PRD-ETL")
        self.server_port = config.get("server_port", 9446)
        self.username = config.get("username", "dsadm")
        self.password = config.get("password", "")
        
        # 환경 변수 설정 (DataStage 명령줄 도구가 필요로 함)
        self._setup_environment()
    
    def _setup_environment(self):
        """DataStage 환경 변수 설정"""
        # DataStage 환경 변수 설정
        # 일반적으로 DSHOME, DS_INSTALL_DIR 등이 필요할 수 있음
        ds_home = Path("C:/IBM/InformationServer")
        if ds_home.exists():
            os.environ["DSHOME"] = str(ds_home)
            os.environ["DS_INSTALL_DIR"] = str(ds_home)
            # PATH에 Classic 디렉토리 추가
            classic_path = str(self.classic_path)
            if classic_path not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{classic_path};{os.environ.get('PATH', '')}"
    
    def _run_dsjob_command(self, *args) -> Dict[str, Any]:
        """
        dsjob 명령어 실행
        
        Args:
            *args: dsjob 명령어 인자들
        
        Returns:
            실행 결과 딕셔너리
        """
        if not self.dsjob_exe.exists():
            logger.error(f"dsjob.exe를 찾을 수 없습니다: {self.dsjob_exe}")
            return {"success": False, "output": "", "error": "dsjob.exe not found"}
        
        try:
            # dsjob 명령어 구성
            # 형식: dsjob -server <host>:<port> -user <user> -password <pass> <primary_command> <args>
            # primary_command는 -ljobs, -lprojects 등
            cmd = [
                str(self.dsjob_exe),
                "-server", f"{self.server_host}:{self.server_port}",
                "-user", self.username,
                "-password", self.password,
            ] + list(args)
            
            logger.debug(f"dsjob 명령어 실행: {' '.join(cmd[:6])}...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='ignore'
            )
            
            return {
                "success": result.returncode == 0,
                "output": result.stdout,
                "error": result.stderr,
                "exit_code": result.returncode
            }
        except subprocess.TimeoutExpired:
            logger.error("dsjob 명령어 실행 타임아웃")
            return {"success": False, "output": "", "error": "Timeout"}
        except Exception as e:
            logger.error(f"dsjob 명령어 실행 실패: {e}")
            return {"success": False, "output": "", "error": str(e)}
    
    def get_projects(self) -> List[Dict[str, Any]]:
        """
        프로젝트 목록 조회
        
        Returns:
            프로젝트 정보 리스트
        """
        # dsjob으로 프로젝트 목록 조회 시도
        # dsjob -listprojects 명령어는 없을 수 있으므로 다른 방법 시도
        projects = []
        
        # 방법 1: dsadmin 사용
        if self.dsadmin_exe.exists():
            try:
                cmd = [
                    str(self.dsadmin_exe),
                    "-server", f"{self.server_host}:{self.server_port}",
                    "-user", self.username,
                    "-password", self.password,
                    "-listprojects"
                ]
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    encoding='utf-8',
                    errors='ignore'
                )
                
                if result.returncode == 0:
                    for line in result.stdout.split('\n'):
                        line = line.strip()
                        if line and not line.startswith('#'):
                            projects.append({
                                "name": line,
                                "source": "dsadmin"
                            })
                    if projects:
                        logger.info(f"dsadmin으로 {len(projects)}개 프로젝트 발견")
                        return projects
            except Exception as e:
                logger.debug(f"dsadmin 실행 실패: {e}")
        
        # 방법 2: 기본 프로젝트 반환
        config = get_config().get_datastage_config()
        default_project = config.get("default_project", "BIDW_ADM")
        if default_project:
            projects.append({
                "name": default_project,
                "source": "config"
            })
        
        return projects
    
    def get_jobs(self, project_name: str) -> List[Dict[str, Any]]:
        """
        Job 목록 조회
        
        Args:
            project_name: 프로젝트 이름
        
        Returns:
            Job 정보 리스트
        """
        jobs = []
        
        # dsjob -ljobs 명령어 사용 (올바른 형식)
        result = self._run_dsjob_command("-ljobs", project_name)
        
        if result["success"]:
            # dsjob 출력 파싱
            for line in result["output"].split('\n'):
                line = line.strip()
                if line and not line.startswith('#') and line:
                    # Job 이름 추출
                    # 형식: "JobName" 또는 "JobName Status" 등
                    parts = line.split()
                    if parts:
                        job_name = parts[0].strip()
                        # 빈 문자열이나 헤더 제외
                        if job_name and job_name.lower() not in ['job', 'jobs', 'name']:
                            jobs.append({
                                "name": job_name,
                                "project": project_name,
                                "source": "dsjob"
                            })
            
            if jobs:
                logger.info(f"dsjob으로 {len(jobs)}개 Job 발견: {project_name}")
                return jobs
        else:
            logger.debug(f"dsjob -ljobs 실패: {result.get('error', 'Unknown error')}")
            logger.debug(f"dsjob 출력: {result.get('output', '')}")
        
        return []
    
    def get_job_definition(self, project_name: str, job_name: str) -> Optional[Dict[str, Any]]:
        """
        Job 정의 조회 (Export 사용)
        
        Args:
            project_name: 프로젝트 이름
            job_name: Job 이름
        
        Returns:
            Job 정의 딕셔너리
        """
        # dsexport를 사용하여 Job Export
        if not self.dsexport_exe.exists():
            logger.error(f"dsexport.exe를 찾을 수 없습니다: {self.dsexport_exe}")
            return None
        
        try:
            # 임시 파일 경로
            import tempfile
            temp_dir = Path(tempfile.gettempdir())
            temp_file = temp_dir / f"{job_name}_export.dsx"
            
            # dsexport 명령어 실행
            cmd = [
                str(self.dsexport_exe),
                "-server", f"{self.server_host}:{self.server_port}",
                "-user", self.username,
                "-password", self.password,
                "-job", f"{project_name}.{job_name}",
                "-file", str(temp_file)
            ]
            
            logger.debug(f"dsexport 실행: {job_name}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0 and temp_file.exists():
                # Export 파일 파싱
                from src.datastage.dsx_parser import DSXParser
                parser = DSXParser()
                job_info = parser.parse_dsx_file(str(temp_file))
                
                # 임시 파일 삭제
                try:
                    temp_file.unlink()
                except:
                    pass
                
                if job_info:
                    logger.info(f"Job 정의 조회 성공: {project_name}/{job_name}")
                    return job_info
            else:
                logger.error(f"dsexport 실패: {result.stderr}")
        
        except Exception as e:
            logger.error(f"Job 정의 조회 실패: {e}")
            import traceback
            logger.debug(traceback.format_exc())
        
        return None
    
    def test_connection(self) -> Dict[str, Any]:
        """
        연결 테스트
        
        Returns:
            연결 테스트 결과 딕셔너리
        """
        result = {
            "success": False,
            "method": "designer_client",
            "dsjob_exists": self.dsjob_exe.exists(),
            "dsexport_exists": self.dsexport_exe.exists(),
            "dsadmin_exists": self.dsadmin_exe.exists(),
            "error": None
        }
        
        if not self.dsjob_exe.exists():
            result["error"] = "dsjob.exe를 찾을 수 없습니다"
            return result
        
        # 간단한 명령어로 연결 테스트 (-lprojects 사용)
        test_result = self._run_dsjob_command("-lprojects")
        
        if test_result["success"]:
            result["success"] = True
            logger.info("DataStage Designer 클라이언트 연결 성공")
        else:
            result["error"] = test_result.get("error", "연결 실패")
            logger.warning(f"DataStage Designer 클라이언트 연결 실패: {result['error']}")
        
        return result
    
    def import_job(self, dsx_file_path: str, project_name: Optional[str] = None, 
                   overwrite: bool = False) -> Dict[str, Any]:
        """
        DSX 파일을 DataStage에 Import
        
        Args:
            dsx_file_path: Import할 DSX 파일 경로
            project_name: 프로젝트 이름 (None이면 DSX 파일에서 추출)
            overwrite: 기존 Job 덮어쓰기 여부
        
        Returns:
            Import 결과 딕셔너리
        """
        result = {
            "success": False,
            "error": None,
            "output": None
        }
        
        if not self.dsimport_exe.exists():
            result["error"] = f"dsimport.exe를 찾을 수 없습니다: {self.dsimport_exe}"
            logger.error(result["error"])
            return result
        
        dsx_path = Path(dsx_file_path)
        if not dsx_path.exists():
            result["error"] = f"DSX 파일이 존재하지 않습니다: {dsx_file_path}"
            logger.error(result["error"])
            return result
        
        try:
            # dsimport 명령어 구성
            cmd = [
                str(self.dsimport_exe),
                "-server", f"{self.server_host}:{self.server_port}",
                "-user", self.username,
                "-password", self.password,
                "-file", str(dsx_path.absolute())
            ]
            
            if project_name:
                cmd.extend(["-project", project_name])
            
            if overwrite:
                cmd.append("-overwrite")
            
            logger.info(f"dsimport 실행: {dsx_path.name}")
            logger.debug(f"명령어: {' '.join(cmd[:4])} ... -file {dsx_path.name}")
            
            # 명령어 실행
            process_result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5분 타임아웃
                encoding='utf-8',
                errors='ignore',
                cwd=str(self.classic_path)
            )
            
            result["output"] = process_result.stdout
            result["error_output"] = process_result.stderr
            
            if process_result.returncode == 0:
                result["success"] = True
                logger.info(f"Job Import 성공: {dsx_path.name}")
            else:
                result["error"] = f"dsimport 실패 (exit code: {process_result.returncode})"
                logger.error(f"{result['error']}: {process_result.stderr}")
        
        except subprocess.TimeoutExpired:
            result["error"] = "dsimport 타임아웃 (5분 초과)"
            logger.error(result["error"])
        except Exception as e:
            result["error"] = f"dsimport 실행 실패: {e}"
            logger.error(result["error"])
            import traceback
            logger.debug(traceback.format_exc())
        
        return result
    
    def export_project(self, project_name: str, output_file: str, 
                      include_jobs: bool = True,
                      include_sequences: bool = True) -> Dict[str, Any]:
        """
        프로젝트 전체 Export
        
        Args:
            project_name: 프로젝트 이름
            output_file: 출력 파일 경로
            include_jobs: Job 포함 여부
            include_sequences: Sequence 포함 여부
        
        Returns:
            Export 결과 딕셔너리
        """
        result = {
            "success": False,
            "error": None,
            "output": None
        }
        
        if not self.dsexport_exe.exists():
            result["error"] = f"dsexport.exe를 찾을 수 없습니다: {self.dsexport_exe}"
            logger.error(result["error"])
            return result
        
        try:
            # dsexport 명령어 구성
            cmd = [
                str(self.dsexport_exe),
                "-server", f"{self.server_host}:{self.server_port}",
                "-user", self.username,
                "-password", self.password,
                "-project", project_name,
                "-file", str(Path(output_file).absolute())
            ]
            
            if include_jobs:
                cmd.append("-jobs")
            if include_sequences:
                cmd.append("-sequences")
            
            logger.info(f"프로젝트 Export 시작: {project_name} → {output_file}")
            logger.debug(f"명령어: {' '.join(cmd[:4])} ... -project {project_name} -file {Path(output_file).name}")
            
            # 명령어 실행
            process_result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,  # 10분 타임아웃
                encoding='utf-8',
                errors='ignore',
                cwd=str(self.classic_path)
            )
            
            result["output"] = process_result.stdout
            result["error_output"] = process_result.stderr
            
            if process_result.returncode == 0:
                output_path = Path(output_file)
                if output_path.exists():
                    result["success"] = True
                    result["file_size"] = output_path.stat().st_size
                    logger.info(f"프로젝트 Export 성공: {project_name} ({result['file_size']} bytes)")
                else:
                    result["error"] = "Export 파일이 생성되지 않았습니다"
                    logger.error(result["error"])
            else:
                result["error"] = f"dsexport 실패 (exit code: {process_result.returncode})"
                logger.error(f"{result['error']}: {process_result.stderr}")
        
        except subprocess.TimeoutExpired:
            result["error"] = "dsexport 타임아웃 (10분 초과)"
            logger.error(result["error"])
        except Exception as e:
            result["error"] = f"dsexport 실행 실패: {e}"
            logger.error(result["error"])
            import traceback
            logger.debug(traceback.format_exc())
        
        return result

