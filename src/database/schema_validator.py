"""스키마 무결성 검증 모듈"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

from src.database.connectors import get_connector
from src.datastage.job_parser import JobParser
from src.core.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationIssue:
    """검증 이슈 데이터 클래스"""
    severity: str  # "error", "warning", "info"
    issue_type: str  # "trimming", "pk_logic", "length_mismatch", etc.
    message: str
    table_name: str
    column_name: Optional[str] = None
    etl_length: Optional[int] = None
    db_length: Optional[int] = None
    recommendation: Optional[str] = None


class SchemaValidator:
    """스키마 무결성 검증 클래스"""
    
    def __init__(self, db_type: str = "mssql"):
        """
        스키마 검증자 초기화
        
        Args:
            db_type: 데이터베이스 타입 ("mssql" 또는 "vertica")
        """
        self.db_connector = get_connector(db_type)
        self.job_parser = JobParser()
    
    def validate_job_schema(
        self,
        job_definition: Dict[str, Any],
        project_name: Optional[str] = None
    ) -> List[ValidationIssue]:
        """
        ETL Job의 스키마 무결성 검증
        
        Args:
            job_definition: Job 정의
            project_name: 프로젝트 이름 (선택)
        
        Returns:
            검증 이슈 리스트
        """
        logger.info("스키마 검증 시작")
        
        issues = []
        
        # Job 메타데이터 파싱
        metadata = self.job_parser.parse_job_definition(job_definition)
        
        # Target 테이블에 대해 검증
        for target_table in metadata.get("target_tables", []):
            table_name = target_table.get("table_name")
            schema = target_table.get("schema", "dbo" if self.db_connector.__class__.__name__ == "MSSQLConnector" else "public")
            
            if not table_name:
                continue
            
            # DB 스키마 조회
            try:
                db_schema = self.db_connector.get_table_schema(table_name, schema)
            except Exception as e:
                logger.error(f"테이블 스키마 조회 실패: {schema}.{table_name} - {e}")
                issues.append(ValidationIssue(
                    severity="error",
                    issue_type="schema_fetch_error",
                    message=f"테이블 스키마를 조회할 수 없습니다: {e}",
                    table_name=table_name
                ))
                continue
            
            # Link와 DB 스키마 비교
            for link in metadata.get("links", []):
                # Target Stage로 가는 Link만 검증
                if link.get("target_stage") == target_table.get("stage_name"):
                    link_issues = self._validate_link_against_db(
                        link,
                        db_schema,
                        table_name,
                        schema
                    )
                    issues.extend(link_issues)
        
        logger.info(f"스키마 검증 완료: {len(issues)}개 이슈 발견")
        return issues
    
    def _validate_link_against_db(
        self,
        link: Dict[str, Any],
        db_schema: List[Dict[str, Any]],
        table_name: str,
        schema: str
    ) -> List[ValidationIssue]:
        """
        Link와 DB 스키마 비교 검증
        
        Args:
            link: Link 정보
            db_schema: DB 테이블 스키마
            table_name: 테이블 이름
            schema: 스키마 이름
        
        Returns:
            검증 이슈 리스트
        """
        issues = []
        
        # DB 스키마를 딕셔너리로 변환 (빠른 조회를 위해)
        db_columns = {col["name"].lower(): col for col in db_schema}
        
        for etl_column in link.get("columns", []):
            col_name = etl_column.get("name", "")
            col_name_lower = col_name.lower()
            
            if col_name_lower not in db_columns:
                # DB에 없는 컬럼
                issues.append(ValidationIssue(
                    severity="warning",
                    issue_type="missing_column",
                    message=f"ETL Link에 있지만 DB에 없는 컬럼: {col_name}",
                    table_name=table_name,
                    column_name=col_name
                ))
                continue
            
            db_column = db_columns[col_name_lower]
            etl_length = etl_column.get("length")
            db_length = db_column.get("length")
            
            # Trimming Check: 길이 불일치 검증
            if etl_length and db_length:
                if etl_length > db_length:
                    issues.append(ValidationIssue(
                        severity="error",
                        issue_type="trimming",
                        message=f"데이터 잘림 위험: ETL Link 길이({etl_length}) > DB 컬럼 길이({db_length})",
                        table_name=table_name,
                        column_name=col_name,
                        etl_length=etl_length,
                        db_length=db_length,
                        recommendation=f"DB 컬럼 길이를 {etl_length} 이상으로 변경하거나 ETL Link 길이를 {db_length} 이하로 조정하세요"
                    ))
                elif etl_length < db_length:
                    issues.append(ValidationIssue(
                        severity="warning",
                        issue_type="length_mismatch",
                        message=f"길이 불일치: ETL Link 길이({etl_length}) < DB 컬럼 길이({db_length})",
                        table_name=table_name,
                        column_name=col_name,
                        etl_length=etl_length,
                        db_length=db_length
                    ))
            
            # PK/Unique 무결성 로직 검증
            if db_column.get("is_pk"):
                # PK 컬럼에 TRIM/UPPER 함수가 적용되어 있는지 확인
                # (실제로는 Job 정의에서 변환 로직을 확인해야 함)
                # 여기서는 기본적인 경고만 제공
                pass
        
        return issues
    
    def validate_pk_logic(
        self,
        job_definition: Dict[str, Any],
        table_name: str,
        schema: str
    ) -> List[ValidationIssue]:
        """
        PK/Unique 무결성 로직 검증
        
        Args:
            job_definition: Job 정의
            table_name: 테이블 이름
            schema: 스키마 이름
        
        Returns:
            검증 이슈 리스트
        """
        issues = []
        
        # DB 스키마 조회
        try:
            db_schema = self.db_connector.get_table_schema(table_name, schema)
        except Exception as e:
            logger.error(f"테이블 스키마 조회 실패: {e}")
            return issues
        
        # PK 컬럼 찾기
        pk_columns = [col for col in db_schema if col.get("is_pk")]
        
        if not pk_columns:
            return issues
        
        # Job 정의를 문자열로 변환하여 TRIM/UPPER 함수 사용 확인
        import json
        job_str = json.dumps(job_definition) if isinstance(job_definition, dict) else str(job_definition)
        job_str_lower = job_str.lower()
        
        for pk_col in pk_columns:
            col_name = pk_col["name"]
            
            # PK 컬럼에 TRIM/UPPER가 적용되어 있는지 확인
            # (실제로는 더 정교한 파싱이 필요)
            if "trim" in job_str_lower and col_name.lower() in job_str_lower:
                issues.append(ValidationIssue(
                    severity="warning",
                    issue_type="pk_logic",
                    message=f"PK 컬럼 '{col_name}'에 TRIM() 함수가 적용되어 논리적 중복 가능성이 있습니다",
                    table_name=table_name,
                    column_name=col_name,
                    recommendation="PK 컬럼에는 TRIM() 함수를 사용하지 않는 것을 권장합니다"
                ))
            
            if "upper" in job_str_lower and col_name.lower() in job_str_lower:
                issues.append(ValidationIssue(
                    severity="info",
                    issue_type="pk_logic",
                    message=f"PK 컬럼 '{col_name}'에 UPPER() 함수가 적용되어 있습니다",
                    table_name=table_name,
                    column_name=col_name
                ))
        
        return issues
    
    def generate_validation_report(
        self,
        issues: List[ValidationIssue]
    ) -> Dict[str, Any]:
        """
        검증 리포트 생성
        
        Args:
            issues: 검증 이슈 리스트
        
        Returns:
            리포트 딕셔너리
        """
        report = {
            "total_issues": len(issues),
            "by_severity": {
                "error": len([i for i in issues if i.severity == "error"]),
                "warning": len([i for i in issues if i.severity == "warning"]),
                "info": len([i for i in issues if i.severity == "info"])
            },
            "by_type": {},
            "issues": []
        }
        
        # 이슈 타입별 집계
        for issue in issues:
            if issue.issue_type not in report["by_type"]:
                report["by_type"][issue.issue_type] = 0
            report["by_type"][issue.issue_type] += 1
        
        # 이슈 상세 정보
        for issue in issues:
            report["issues"].append({
                "severity": issue.severity,
                "type": issue.issue_type,
                "message": issue.message,
                "table": issue.table_name,
                "column": issue.column_name,
                "etl_length": issue.etl_length,
                "db_length": issue.db_length,
                "recommendation": issue.recommendation
            })
        
        return report

