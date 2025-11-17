"""DataStage Job 자동 수정 모듈"""

from typing import Dict, Any, List, Optional, Callable
from pathlib import Path

from src.core.logger import get_logger
from src.datastage.dsx_editor import DSXEditor
from src.datastage.dependency_analyzer import DependencyAnalyzer

logger = get_logger(__name__)


class JobModifier:
    """DataStage Job 자동 수정 클래스"""
    
    def __init__(self, export_directory: Optional[str] = None):
        """
        Job 수정기 초기화
        
        Args:
            export_directory: Export된 DSX 파일이 있는 디렉토리
        """
        self.export_directory = Path(export_directory) if export_directory else None
        self.analyzer = DependencyAnalyzer(export_directory)
        self.modified_jobs: List[Dict[str, Any]] = []
    
    def modify_table_name(self, old_table: str, new_table: str,
                         old_schema: Optional[str] = None,
                         new_schema: Optional[str] = None,
                         export_directory: Optional[str] = None) -> Dict[str, Any]:
        """
        테이블 이름 변경에 따른 연관 Job 수정
        
        Args:
            old_table: 기존 테이블 이름
            new_table: 새로운 테이블 이름
            old_schema: 기존 스키마 이름
            new_schema: 새로운 스키마 이름
            export_directory: Export 디렉토리
        
        Returns:
            수정 결과 딕셔너리
        """
        result = {
            "success": True,
            "modified_jobs": [],
            "failed_jobs": [],
            "total_modified": 0
        }
        
        # 연관 Job 찾기
        related_jobs = self.analyzer.find_jobs_using_table(
            table_name=old_table,
            schema=old_schema,
            export_directory=export_directory
        )
        
        logger.info(f"테이블 이름 변경: '{old_schema or ''}.{old_table}' → '{new_schema or ''}.{new_table}'")
        logger.info(f"연관 Job {len(related_jobs)}개 발견")
        
        # 각 Job 수정
        for job_info in related_jobs:
            job_file = job_info.get("file_path")
            job_name = job_info.get("job_name", "Unknown")
            
            try:
                editor = DSXEditor(job_file)
                
                # 테이블 이름 변경
                count = editor.replace_table_name(
                    old_table=old_table,
                    new_table=new_table,
                    old_schema=old_schema,
                    new_schema=new_schema
                )
                
                if count > 0:
                    # 수정된 파일 저장
                    output_file = str(Path(job_file).with_suffix('.modified.dsx'))
                    if editor.save(output_file, backup=True):
                        result["modified_jobs"].append({
                            "job_name": job_name,
                            "original_file": job_file,
                            "modified_file": output_file,
                            "changes_count": count
                        })
                        result["total_modified"] += count
                        logger.info(f"✓ Job 수정 완료: {job_name} ({count}개 변경)")
                    else:
                        result["failed_jobs"].append({
                            "job_name": job_name,
                            "file": job_file,
                            "error": "파일 저장 실패"
                        })
                else:
                    logger.debug(f"변경사항 없음: {job_name}")
            
            except Exception as e:
                result["failed_jobs"].append({
                    "job_name": job_name,
                    "file": job_file,
                    "error": str(e)
                })
                logger.error(f"Job 수정 실패: {job_name} - {e}")
        
        result["success"] = len(result["failed_jobs"]) == 0
        return result
    
    def modify_column_name(self, table_name: str, old_column: str, new_column: str,
                          schema: Optional[str] = None,
                          export_directory: Optional[str] = None) -> Dict[str, Any]:
        """
        컬럼 이름 변경에 따른 연관 Job 수정
        
        Args:
            table_name: 테이블 이름
            old_column: 기존 컬럼 이름
            new_column: 새로운 컬럼 이름
            schema: 스키마 이름
            export_directory: Export 디렉토리
        
        Returns:
            수정 결과 딕셔너리
        """
        result = {
            "success": True,
            "modified_jobs": [],
            "failed_jobs": [],
            "total_modified": 0
        }
        
        # 연관 Job 찾기
        related_jobs = self.analyzer.find_jobs_using_column(
            table_name=table_name,
            column_name=old_column,
            schema=schema,
            export_directory=export_directory
        )
        
        logger.info(f"컬럼 이름 변경: '{schema or ''}.{table_name}.{old_column}' → '{new_column}'")
        logger.info(f"연관 Job {len(related_jobs)}개 발견")
        
        # 각 Job 수정
        for job_info in related_jobs:
            job_file = job_info.get("file_path")
            job_name = job_info.get("job_name", "Unknown")
            
            try:
                editor = DSXEditor(job_file)
                
                # 컬럼 이름 변경 (DSX에서 컬럼은 여러 곳에 나타날 수 있음)
                # Column "COLUMN_NAME" 패턴 찾아서 변경
                count = editor.replace_value(
                    key="Column",
                    old_value=old_column,
                    new_value=new_column
                )
                
                if count > 0:
                    # 수정된 파일 저장
                    output_file = str(Path(job_file).with_suffix('.modified.dsx'))
                    if editor.save(output_file, backup=True):
                        result["modified_jobs"].append({
                            "job_name": job_name,
                            "original_file": job_file,
                            "modified_file": output_file,
                            "changes_count": count
                        })
                        result["total_modified"] += count
                        logger.info(f"✓ Job 수정 완료: {job_name} ({count}개 변경)")
                    else:
                        result["failed_jobs"].append({
                            "job_name": job_name,
                            "file": job_file,
                            "error": "파일 저장 실패"
                        })
                else:
                    logger.debug(f"변경사항 없음: {job_name}")
            
            except Exception as e:
                result["failed_jobs"].append({
                    "job_name": job_name,
                    "file": job_file,
                    "error": str(e)
                })
                logger.error(f"Job 수정 실패: {job_name} - {e}")
        
        result["success"] = len(result["failed_jobs"]) == 0
        return result
    
    def delete_column(self, table_name: str, column_name: str,
                     schema: Optional[str] = None,
                     export_directory: Optional[str] = None) -> Dict[str, Any]:
        """
        컬럼 삭제에 따른 연관 Job 수정 (컬럼 참조 제거)
        
        Args:
            table_name: 테이블 이름
            column_name: 삭제할 컬럼 이름
            schema: 스키마 이름
            export_directory: Export 디렉토리
        
        Returns:
            수정 결과 딕셔너리
        """
        result = {
            "success": True,
            "modified_jobs": [],
            "failed_jobs": [],
            "total_modified": 0,
            "warnings": []
        }
        
        # 연관 Job 찾기
        related_jobs = self.analyzer.find_jobs_using_column(
            table_name=table_name,
            column_name=column_name,
            schema=schema,
            export_directory=export_directory
        )
        
        logger.warning(f"컬럼 삭제: '{schema or ''}.{table_name}.{column_name}'")
        logger.info(f"연관 Job {len(related_jobs)}개 발견 (수동 검토 필요)")
        
        # 각 Job에서 컬럼 참조 제거 (주의: 수동 검토 필요)
        for job_info in related_jobs:
            job_file = job_info.get("file_path")
            job_name = job_info.get("job_name", "Unknown")
            
            result["warnings"].append({
                "job_name": job_name,
                "file": job_file,
                "message": f"컬럼 '{column_name}' 사용 중 - 수동 검토 필요"
            })
        
        result["success"] = True
        return result
    
    def modify_column_type(self, table_name: str, column_name: str, new_type: str,
                          new_length: Optional[str] = None, new_scale: Optional[str] = None,
                          schema: Optional[str] = None,
                          export_directory: Optional[str] = None) -> Dict[str, Any]:
        """
        컬럼 타입 변경에 따른 연관 Job 수정 (제한적 지원)
        
        주의: 컬럼 타입 변경은 DSX 파일 구조가 복잡하여 자동 수정이 제한적입니다.
        수동 검토가 필요합니다.
        
        Args:
            table_name: 테이블 이름
            column_name: 컬럼 이름
            new_type: 새로운 데이터 타입
            new_length: 새로운 길이/정밀도
            new_scale: 새로운 소수점
            schema: 스키마 이름
            export_directory: Export 디렉토리
        
        Returns:
            수정 결과 딕셔너리
        """
        result = {
            "success": True,
            "modified_jobs": [],
            "failed_jobs": [],
            "total_modified": 0,
            "warnings": []
        }
        
        # 연관 Job 찾기
        related_jobs = self.analyzer.find_jobs_using_column(
            table_name=table_name,
            column_name=column_name,
            schema=schema,
            export_directory=export_directory
        )
        
        logger.warning(f"컬럼 타입 변경: '{schema or ''}.{table_name}.{column_name}' → {new_type}")
        logger.info(f"연관 Job {len(related_jobs)}개 발견 (수동 검토 필요)")
        
        # 각 Job에 경고 추가
        for job_info in related_jobs:
            job_name = job_info.get("job_name", "Unknown")
            result["warnings"].append({
                "job_name": job_name,
                "file": job_info.get("file_path", ""),
                "message": f"컬럼 타입 변경은 수동 검토 필요: {column_name} → {new_type}"
            })
        
        result["success"] = True
        return result
    
    def add_column(self, table_name: str, column_name: str, column_type: str,
                  column_length: Optional[str] = None, column_scale: Optional[str] = None,
                  nullable: bool = True, default_value: Optional[str] = None,
                  schema: Optional[str] = None,
                  export_directory: Optional[str] = None) -> Dict[str, Any]:
        """
        컬럼 추가에 따른 연관 Job 수정 (제한적 지원)
        
        주의: 컬럼 추가는 DSX 파일 구조가 복잡하여 자동 수정이 제한적입니다.
        수동 검토가 필요합니다.
        
        Args:
            table_name: 테이블 이름
            column_name: 추가할 컬럼 이름
            column_type: 데이터 타입
            column_length: 길이/정밀도
            column_scale: 소수점
            nullable: NULL 허용 여부
            default_value: 기본값
            schema: 스키마 이름
            export_directory: Export 디렉토리
        
        Returns:
            수정 결과 딕셔너리
        """
        result = {
            "success": True,
            "modified_jobs": [],
            "failed_jobs": [],
            "total_modified": 0,
            "warnings": []
        }
        
        # 연관 Job 찾기 (해당 테이블을 사용하는 모든 Job)
        related_jobs = self.analyzer.find_jobs_using_table(
            table_name=table_name,
            schema=schema,
            export_directory=export_directory
        )
        
        logger.info(f"컬럼 추가: '{schema or ''}.{table_name}.{column_name}' ({column_type})")
        logger.info(f"연관 Job {len(related_jobs)}개 발견 (수동 검토 필요)")
        
        # 각 Job에 경고 추가
        for job_info in related_jobs:
            job_name = job_info.get("job_name", "Unknown")
            result["warnings"].append({
                "job_name": job_name,
                "file": job_info.get("file_path", ""),
                "message": f"컬럼 추가는 수동 검토 필요: {column_name} ({column_type})"
            })
        
        result["success"] = True
        return result
    
    def generate_modification_report(self, result: Dict[str, Any]) -> str:
        """
        수정 결과 리포트 생성
        
        Args:
            result: 수정 결과 딕셔너리
        
        Returns:
            리포트 문자열
        """
        report = []
        report.append("=" * 60)
        report.append("DataStage Job 수정 리포트")
        report.append("=" * 60)
        report.append("")
        
        report.append(f"전체 성공: {'✓' if result.get('success') else '✗'}")
        report.append(f"수정된 Job 수: {len(result.get('modified_jobs', []))}")
        report.append(f"실패한 Job 수: {len(result.get('failed_jobs', []))}")
        report.append(f"총 변경사항: {result.get('total_modified', 0)}개")
        report.append("")
        
        if result.get("modified_jobs"):
            report.append("수정된 Job 목록:")
            report.append("-" * 60)
            for job in result["modified_jobs"]:
                report.append(f"  ✓ {job['job_name']}")
                if job.get('original_file'):
                    report.append(f"    원본: {Path(job['original_file']).name}")
                if job.get('modified_file'):
                    report.append(f"    수정: {Path(job['modified_file']).name}")
                if job.get('changes_count', 0) > 0:
                    report.append(f"    변경: {job['changes_count']}개")
                if job.get('note'):
                    report.append(f"    참고: {job['note']}")
                report.append("")
        
        if result.get("failed_jobs"):
            report.append("실패한 Job 목록:")
            report.append("-" * 60)
            for job in result["failed_jobs"]:
                report.append(f"  ✗ {job['job_name']}")
                report.append(f"    파일: {Path(job['file']).name}")
                report.append(f"    오류: {job.get('error', 'Unknown')}")
                report.append("")
        
        if result.get("warnings"):
            report.append("경고:")
            report.append("-" * 60)
            for warning in result["warnings"]:
                report.append(f"  ⚠ {warning.get('job_name', 'Unknown')}: {warning.get('message', '')}")
                if warning.get('file'):
                    report.append(f"     파일: {Path(warning['file']).name}")
                report.append("")
        
        report.append("=" * 60)
        return "\n".join(report)

