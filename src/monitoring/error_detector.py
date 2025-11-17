"""오류 코드 감지 및 분석 모듈"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from src.monitoring.log_parser import LogParser, LogError
from src.core.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ErrorAnalysis:
    """오류 분석 결과"""
    error_code: str
    error_type: str
    description: str
    root_cause: str
    affected_tables: List[str]
    affected_columns: List[str]
    recommendations: List[str]
    severity: str  # "critical", "high", "medium", "low"


class ErrorDetector:
    """오류 감지 및 분석 클래스"""
    
    def __init__(self):
        """오류 감지자 초기화"""
        self.log_parser = LogParser()
        
        # 오류 코드별 분석 정보
        self.error_analysis_db = {
            "23505": {
                "type": "sqlstate",
                "description": "Unique constraint violation (PK 충돌)",
                "root_cause": "중복된 키 값이 삽입되거나 업데이트되었습니다",
                "recommendations": [
                    "중복된 데이터 확인",
                    "PK 컬럼에 TRIM() 함수가 적용되어 있는지 확인",
                    "소스 데이터의 중복 여부 확인",
                    "ETL Job의 중복 제거 로직 확인"
                ],
                "severity": "critical"
            },
            "23503": {
                "type": "sqlstate",
                "description": "Foreign key constraint violation",
                "root_cause": "참조 무결성 제약 조건 위반",
                "recommendations": [
                    "참조하는 테이블의 데이터 존재 여부 확인",
                    "FK 제약 조건 확인",
                    "데이터 삽입 순서 확인"
                ],
                "severity": "high"
            },
            "22001": {
                "type": "sqlstate",
                "description": "String data right truncated",
                "root_cause": "데이터 길이가 컬럼 최대 길이를 초과했습니다",
                "recommendations": [
                    "ETL Link 길이와 DB 컬럼 길이 비교",
                    "DB 컬럼 길이 확장 또는 데이터 자르기 로직 추가"
                ],
                "severity": "high"
            },
            "SIGKILL": {
                "type": "system",
                "description": "프로세스 강제 종료",
                "root_cause": "시스템 리소스 부족 또는 관리자에 의한 종료",
                "recommendations": [
                    "시스템 리소스 확인 (메모리, CPU)",
                    "Job 실행 시간 확인",
                    "데이터 볼륨 확인"
                ],
                "severity": "critical"
            }
        }
    
    def analyze_errors(self, errors: List[LogError]) -> List[ErrorAnalysis]:
        """
        오류 분석
        
        Args:
            errors: 로그 오류 리스트
        
        Returns:
            오류 분석 결과 리스트
        """
        logger.info(f"오류 분석 시작: {len(errors)}개 오류")
        
        analyses = []
        
        # 오류 코드별로 그룹화
        grouped_errors = self.log_parser.group_errors_by_code(errors)
        
        for error_code, error_list in grouped_errors.items():
            analysis = self._analyze_error_code(error_code, error_list)
            if analysis:
                analyses.append(analysis)
        
        logger.info(f"오류 분석 완료: {len(analyses)}개 분석 결과")
        return analyses
    
    def _analyze_error_code(self, error_code: str, errors: List[LogError]) -> Optional[ErrorAnalysis]:
        """
        특정 오류 코드 분석
        
        Args:
            error_code: 오류 코드
            errors: 해당 오류 코드의 오류 리스트
        
        Returns:
            오류 분석 결과
        """
        # 분석 정보 조회
        analysis_info = self.error_analysis_db.get(error_code)
        
        if not analysis_info:
            # 알려지지 않은 오류 코드
            analysis_info = {
                "type": errors[0].error_type if errors else "unknown",
                "description": f"알 수 없는 오류 코드: {error_code}",
                "root_cause": "추가 조사 필요",
                "recommendations": ["로그 상세 내용 확인", "DataStage 문서 참조"],
                "severity": "medium"
            }
        
        # 영향받은 테이블/컬럼 수집
        affected_tables = set()
        affected_columns = set()
        
        for error in errors:
            if error.table_name:
                affected_tables.add(error.table_name)
            if error.column_name:
                affected_columns.add(error.column_name)
        
        return ErrorAnalysis(
            error_code=error_code,
            error_type=analysis_info["type"],
            description=analysis_info["description"],
            root_cause=analysis_info["root_cause"],
            affected_tables=list(affected_tables),
            affected_columns=list(affected_columns),
            recommendations=analysis_info["recommendations"],
            severity=analysis_info["severity"]
        )
    
    def generate_error_report(
        self,
        errors: List[LogError],
        analyses: Optional[List[ErrorAnalysis]] = None
    ) -> Dict[str, Any]:
        """
        오류 리포트 생성
        
        Args:
            errors: 오류 정보 리스트
            analyses: 오류 분석 결과 리스트 (None이면 자동 분석)
        
        Returns:
            리포트 딕셔너리
        """
        if analyses is None:
            analyses = self.analyze_errors(errors)
        
        summary = self.log_parser.get_summary(errors)
        
        # 심각도별 집계
        severity_counts = {}
        for analysis in analyses:
            severity = analysis.severity
            if severity not in severity_counts:
                severity_counts[severity] = 0
            severity_counts[severity] += 1
        
        report = {
            "summary": summary,
            "severity_breakdown": severity_counts,
            "analyses": [
                {
                    "error_code": a.error_code,
                    "error_type": a.error_type,
                    "description": a.description,
                    "root_cause": a.root_cause,
                    "affected_tables": a.affected_tables,
                    "affected_columns": a.affected_columns,
                    "recommendations": a.recommendations,
                    "severity": a.severity,
                    "occurrence_count": summary["by_code"].get(a.error_code, 0)
                }
                for a in analyses
            ],
            "critical_issues": [
                a for a in analyses if a.severity == "critical"
            ],
            "recommendations": self._generate_overall_recommendations(analyses)
        }
        
        return report
    
    def _generate_overall_recommendations(self, analyses: List[ErrorAnalysis]) -> List[str]:
        """전체 권장사항 생성"""
        recommendations = []
        
        # 심각도가 높은 오류에 대한 우선 권장사항
        critical_analyses = [a for a in analyses if a.severity == "critical"]
        if critical_analyses:
            recommendations.append("즉시 조치가 필요한 심각한 오류가 발견되었습니다")
        
        # PK 충돌 오류가 있는 경우
        pk_errors = [a for a in analyses if a.error_code == "23505"]
        if pk_errors:
            recommendations.append("PK 충돌 오류: ETL Job의 중복 제거 로직 및 PK 컬럼 변환 함수 확인 필요")
        
        # 데이터 잘림 오류가 있는 경우
        truncation_errors = [a for a in analyses if a.error_code == "22001"]
        if truncation_errors:
            recommendations.append("데이터 잘림 오류: ETL Link 길이와 DB 컬럼 길이 일치 여부 확인 필요")
        
        return recommendations
    
    def detect_common_patterns(self, errors: List[LogError]) -> Dict[str, Any]:
        """
        일반적인 오류 패턴 감지
        
        Args:
            errors: 오류 정보 리스트
        
        Returns:
            패턴 정보 딕셔너리
        """
        patterns = {
            "repeated_errors": {},  # 반복되는 오류
            "time_based_patterns": {},  # 시간대별 패턴
            "table_specific_issues": {}  # 테이블별 이슈
        }
        
        # 반복되는 오류 감지
        error_counts = {}
        for error in errors:
            key = f"{error.error_code}_{error.table_name}"
            if key not in error_counts:
                error_counts[key] = 0
            error_counts[key] += 1
        
        patterns["repeated_errors"] = {
            k: v for k, v in error_counts.items() if v > 1
        }
        
        # 테이블별 이슈 집계
        table_issues = {}
        for error in errors:
            if error.table_name:
                if error.table_name not in table_issues:
                    table_issues[error.table_name] = []
                table_issues[error.table_name].append({
                    "error_code": error.error_code,
                    "error_type": error.error_type,
                    "message": error.message
                })
        
        patterns["table_specific_issues"] = table_issues
        
        return patterns

