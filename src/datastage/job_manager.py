"""DataStage Job 관리 워크플로우 모듈"""

from typing import Dict, Any, List, Optional, Callable
from pathlib import Path

from src.core.config import get_config
from src.core.logger import get_logger
from src.datastage.designer_client import DataStageDesignerClient
from src.datastage.dsx_editor import DSXEditor
from src.datastage.dsx_parser import DSXParser

logger = get_logger(__name__)


class JobManager:
    """DataStage Job 관리 워크플로우 클래스"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Job Manager 초기화
        
        Args:
            config: DataStage 설정 딕셔너리 (None이면 config에서 로드)
        """
        if config is None:
            config = get_config().get_datastage_config()
        
        self.config = config
        self.designer_client = DataStageDesignerClient(config)
        self.dsx_parser = DSXParser()
        self.export_path = Path(config.get("local_export_path", ""))
        
        # Export 파일 저장 디렉토리 생성
        if self.export_path:
            self.export_path.mkdir(parents=True, exist_ok=True)
    
    def export_all_jobs(self, project_name: str, output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        프로젝트의 모든 Job Export
        
        Args:
            project_name: 프로젝트 이름
            output_file: 출력 파일 경로 (None이면 자동 생성)
        
        Returns:
            Export 결과 딕셔너리
        """
        if output_file is None:
            output_file = str(self.export_path / f"{project_name}_AllJobs.dsx")
        
        logger.info(f"프로젝트 전체 Export 시작: {project_name}")
        result = self.designer_client.export_project(
            project_name=project_name,
            output_file=output_file,
            include_jobs=True,
            include_sequences=True
        )
        
        if result["success"]:
            logger.info(f"Export 완료: {output_file} ({result.get('file_size', 0)} bytes)")
        else:
            logger.error(f"Export 실패: {result.get('error', 'Unknown error')}")
        
        return result
    
    def load_job_from_dsx(self, dsx_file_path: str) -> Optional[DSXEditor]:
        """
        DSX 파일에서 Job 로드
        
        Args:
            dsx_file_path: DSX 파일 경로
        
        Returns:
            DSXEditor 인스턴스 또는 None
        """
        try:
            editor = DSXEditor(dsx_file_path)
            logger.info(f"Job 로드 성공: {dsx_file_path}")
            return editor
        except Exception as e:
            logger.error(f"Job 로드 실패: {e}")
            return None
    
    def modify_job(self, dsx_file_path: str, 
                   modifications: List[Callable[[DSXEditor], bool]]) -> Dict[str, Any]:
        """
        Job 수정
        
        Args:
            dsx_file_path: DSX 파일 경로
            modifications: 수정 함수 리스트 (각 함수는 DSXEditor를 받아서 bool 반환)
        
        Returns:
            수정 결과 딕셔너리
        """
        result = {
            "success": False,
            "error": None,
            "modifications_applied": 0,
            "output_file": None
        }
        
        try:
            editor = self.load_job_from_dsx(dsx_file_path)
            if not editor:
                result["error"] = "DSX 파일 로드 실패"
                return result
            
            # 수정 적용
            for mod_func in modifications:
                try:
                    if mod_func(editor):
                        result["modifications_applied"] += 1
                except Exception as e:
                    logger.warning(f"수정 함수 실행 실패: {e}")
            
            # 수정된 파일 저장
            output_file = str(Path(dsx_file_path).with_suffix('.modified.dsx'))
            if editor.save(output_file, backup=True):
                result["success"] = True
                result["output_file"] = output_file
                logger.info(f"Job 수정 완료: {output_file}")
            else:
                result["error"] = "파일 저장 실패"
        
        except Exception as e:
            result["error"] = f"Job 수정 실패: {e}"
            logger.error(result["error"])
            import traceback
            logger.debug(traceback.format_exc())
        
        return result
    
    def import_job(self, dsx_file_path: str, project_name: str, 
                  overwrite: bool = True) -> Dict[str, Any]:
        """
        수정된 Job을 DataStage에 Import
        
        Args:
            dsx_file_path: Import할 DSX 파일 경로
            project_name: 프로젝트 이름
            overwrite: 기존 Job 덮어쓰기 여부
        
        Returns:
            Import 결과 딕셔너리
        """
        logger.info(f"Job Import 시작: {dsx_file_path} → {project_name}")
        result = self.designer_client.import_job(
            dsx_file_path=dsx_file_path,
            project_name=project_name,
            overwrite=overwrite
        )
        
        if result["success"]:
            logger.info(f"Import 완료: {Path(dsx_file_path).name}")
        else:
            logger.error(f"Import 실패: {result.get('error', 'Unknown error')}")
        
        return result
    
    def export_modify_import_workflow(self, 
                                      project_name: str,
                                      job_name: Optional[str] = None,
                                      modifications: Optional[List[Callable[[DSXEditor], bool]]] = None,
                                      overwrite: bool = True) -> Dict[str, Any]:
        """
        전체 워크플로우: Export → 수정 → Import
        
        Args:
            project_name: 프로젝트 이름
            job_name: 특정 Job 이름 (None이면 전체 프로젝트)
            modifications: 수정 함수 리스트
            overwrite: Import 시 덮어쓰기 여부
        
        Returns:
            워크플로우 결과 딕셔너리
        """
        result = {
            "success": False,
            "steps": {
                "export": {"success": False},
                "modify": {"success": False},
                "import": {"success": False}
            },
            "error": None
        }
        
        try:
            # Step 1: Export
            if job_name:
                # 특정 Job Export (추후 구현)
                logger.warning("특정 Job Export는 아직 구현되지 않았습니다. 전체 프로젝트 Export를 사용합니다.")
                export_result = self.export_all_jobs(project_name)
            else:
                export_result = self.export_all_jobs(project_name)
            
            result["steps"]["export"] = export_result
            
            if not export_result["success"]:
                result["error"] = f"Export 실패: {export_result.get('error', 'Unknown error')}"
                return result
            
            export_file = export_result.get("output_file") or f"{project_name}_AllJobs.dsx"
            
            # Step 2: 수정 (수정 함수가 있는 경우)
            if modifications:
                modify_result = self.modify_job(export_file, modifications)
                result["steps"]["modify"] = modify_result
                
                if not modify_result["success"]:
                    result["error"] = f"수정 실패: {modify_result.get('error', 'Unknown error')}"
                    return result
                
                import_file = modify_result["output_file"]
            else:
                import_file = export_file
            
            # Step 3: Import
            import_result = self.import_job(import_file, project_name, overwrite)
            result["steps"]["import"] = import_result
            
            if import_result["success"]:
                result["success"] = True
                logger.info("전체 워크플로우 완료: Export → 수정 → Import")
            else:
                result["error"] = f"Import 실패: {import_result.get('error', 'Unknown error')}"
        
        except Exception as e:
            result["error"] = f"워크플로우 실행 실패: {e}"
            logger.error(result["error"])
            import traceback
            logger.debug(traceback.format_exc())
        
        return result

