"""로컬 Export 파일을 통한 DataStage 클라이언트"""

from typing import Dict, Any, Optional, List
from pathlib import Path

from src.core.config import get_config
from src.core.logger import get_logger
from src.datastage.dsx_parser import DSXParser

logger = get_logger(__name__)


class DataStageLocalClient:
    """로컬 Export 파일을 통한 DataStage 클라이언트"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        로컬 클라이언트 초기화
        
        Args:
            config: DataStage 설정 딕셔너리 (None이면 config에서 로드)
        """
        if config is None:
            config = get_config().get_datastage_config()
        
        self.export_path = config.get("local_export_path", "")
        self.dsx_parser = DSXParser()
    
    def get_jobs(self, project_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        로컬 Export 파일에서 Job 목록 조회
        
        Args:
            project_name: 프로젝트 이름 (선택, None이면 모든 프로젝트)
        
        Returns:
            Job 정보 리스트
        """
        if not self.export_path:
            logger.warning("로컬 Export 경로가 설정되지 않았습니다.")
            return []
        
        try:
            jobs = self.dsx_parser.scan_directory(self.export_path)
            
            # 프로젝트 이름으로 필터링
            if project_name:
                jobs = [
                    job for job in jobs 
                    if job.get("project", "").upper() == project_name.upper()
                ]
            
            return jobs
        except Exception as e:
            logger.error(f"로컬 Job 목록 조회 실패: {e}")
            return []
    
    def get_job_definition(self, job_name: str, project_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        로컬 Export 파일에서 Job 정의 조회
        
        Args:
            job_name: Job 이름
            project_name: 프로젝트 이름 (선택)
        
        Returns:
            Job 정의 딕셔너리
        """
        if not self.export_path:
            return None
        
        try:
            dir_path = Path(self.export_path)
            if not dir_path.exists():
                return None
            
            # Job 이름으로 DSX 파일 찾기 (확장자 포함 및 미포함 모두)
            dsx_files = list(dir_path.glob("*.dsx"))
            # 확장자 없는 파일도 확인
            for file_path in dir_path.iterdir():
                if file_path.is_file() and not file_path.suffix and file_path not in dsx_files:
                    dsx_files.append(file_path)
            
            for dsx_file in dsx_files:
                job_info = self.dsx_parser.parse_dsx_file(str(dsx_file))
                if job_info and job_info.get("name") == job_name:
                    # 프로젝트 필터링
                    if project_name:
                        if job_info.get("project", "").upper() != project_name.upper():
                            continue
                    
                    return job_info
            
            return None
        except Exception as e:
            logger.error(f"로컬 Job 정의 조회 실패: {e}")
            return None

