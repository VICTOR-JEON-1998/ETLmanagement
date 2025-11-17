"""ETL 로그 파싱 모듈"""

import re
from typing import Dict, Any, List, Optional
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

from src.core.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LogError:
    """로그 오류 정보"""
    timestamp: Optional[datetime]
    error_code: str
    error_type: str  # "sqlstate", "system", "datastage", etc.
    message: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    job_name: Optional[str] = None
    stage_name: Optional[str] = None
    raw_line: Optional[str] = None


class LogParser:
    """ETL 로그 파서 클래스"""
    
    # SQLSTATE 패턴
    SQLSTATE_PATTERN = re.compile(r'SQLSTATE[=:\s]+(\d{5})', re.IGNORECASE)
    
    # 일반적인 SQLSTATE 코드
    SQLSTATE_CODES = {
        "23505": "Unique constraint violation (PK 충돌)",
        "23503": "Foreign key constraint violation",
        "22001": "String data right truncated",
        "22003": "Numeric value out of range",
        "42S02": "Base table or view not found",
        "42S22": "Column not found"
    }
    
    # 시스템 오류 패턴
    SIGKILL_PATTERN = re.compile(r'SIGKILL|Killed|terminated|signal\s+\d+', re.IGNORECASE)
    
    # DataStage 오류 패턴
    DATASTAGE_ERROR_PATTERN = re.compile(
        r'(Error|Fatal|Exception|Failed)[:\s]+(.+?)(?:\n|$)',
        re.IGNORECASE | re.MULTILINE
    )
    
    def __init__(self):
        """로그 파서 초기화"""
        pass
    
    def parse_log_file(self, log_file_path: str) -> List[LogError]:
        """
        로그 파일 파싱
        
        Args:
            log_file_path: 로그 파일 경로
        
        Returns:
            오류 정보 리스트
        """
        log_path = Path(log_file_path)
        if not log_path.exists():
            logger.error(f"로그 파일을 찾을 수 없습니다: {log_file_path}")
            return []
        
        logger.info(f"로그 파일 파싱 시작: {log_file_path}")
        
        errors = []
        
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    line_errors = self._parse_line(line, line_num)
                    errors.extend(line_errors)
        except Exception as e:
            logger.error(f"로그 파일 읽기 실패: {e}")
            return []
        
        logger.info(f"로그 파싱 완료: {len(errors)}개 오류 발견")
        return errors
    
    def _parse_line(self, line: str, line_num: int) -> List[LogError]:
        """
        로그 라인 파싱
        
        Args:
            line: 로그 라인
            line_num: 라인 번호
        
        Returns:
            오류 정보 리스트
        """
        errors = []
        
        # 타임스탬프 추출
        timestamp = self._extract_timestamp(line)
        
        # SQLSTATE 코드 검색
        sqlstate_match = self.SQLSTATE_PATTERN.search(line)
        if sqlstate_match:
            sqlstate_code = sqlstate_match.group(1)
            error_msg = self.SQLSTATE_CODES.get(sqlstate_code, f"SQLSTATE {sqlstate_code}")
            
            # 테이블/컬럼 정보 추출
            table_name, column_name = self._extract_table_column_info(line)
            
            errors.append(LogError(
                timestamp=timestamp,
                error_code=sqlstate_code,
                error_type="sqlstate",
                message=error_msg,
                table_name=table_name,
                column_name=column_name,
                raw_line=line.strip()
            ))
        
        # 시스템 오류 검색
        if self.SIGKILL_PATTERN.search(line):
            errors.append(LogError(
                timestamp=timestamp,
                error_code="SIGKILL",
                error_type="system",
                message="프로세스가 강제 종료되었습니다",
                raw_line=line.strip()
            ))
        
        # DataStage 오류 검색
        datastage_match = self.DATASTAGE_ERROR_PATTERN.search(line)
        if datastage_match and not sqlstate_match:  # SQLSTATE와 중복 방지
            error_msg = datastage_match.group(2).strip()
            errors.append(LogError(
                timestamp=timestamp,
                error_code="DATASTAGE_ERROR",
                error_type="datastage",
                message=error_msg,
                raw_line=line.strip()
            ))
        
        return errors
    
    def _extract_timestamp(self, line: str) -> Optional[datetime]:
        """타임스탬프 추출"""
        # 다양한 타임스탬프 형식 지원
        timestamp_patterns = [
            r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})',
            r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})',
            r'(\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\])',
        ]
        
        for pattern in timestamp_patterns:
            match = re.search(pattern, line)
            if match:
                try:
                    ts_str = match.group(1).strip('[]')
                    # 간단한 파싱 (실제로는 더 정교한 파싱 필요)
                    return datetime.strptime(ts_str.split('.')[0], "%Y-%m-%d %H:%M:%S")
                except:
                    pass
        
        return None
    
    def _extract_table_column_info(self, line: str) -> tuple[Optional[str], Optional[str]]:
        """
        테이블/컬럼 정보 추출
        
        Args:
            line: 로그 라인
        
        Returns:
            (table_name, column_name)
        """
        table_name = None
        column_name = None
        
        # 테이블 이름 패턴 (예: "table 'schema.table_name'")
        table_pattern = re.compile(r"table\s+['\"]?(\w+\.\w+)['\"]?", re.IGNORECASE)
        table_match = table_pattern.search(line)
        if table_match:
            table_name = table_match.group(1)
        
        # 컬럼 이름 패턴 (예: "column 'column_name'")
        column_pattern = re.compile(r"column\s+['\"]?(\w+)['\"]?", re.IGNORECASE)
        column_match = column_pattern.search(line)
        if column_match:
            column_name = column_match.group(1)
        
        return table_name, column_name
    
    def parse_log_directory(self, log_dir: str, pattern: str = "*.log") -> List[LogError]:
        """
        로그 디렉토리의 모든 로그 파일 파싱
        
        Args:
            log_dir: 로그 디렉토리 경로
            pattern: 파일 패턴 (기본값: "*.log")
        
        Returns:
            오류 정보 리스트
        """
        log_path = Path(log_dir)
        if not log_path.exists() or not log_path.is_dir():
            logger.error(f"로그 디렉토리를 찾을 수 없습니다: {log_dir}")
            return []
        
        all_errors = []
        
        for log_file in log_path.glob(pattern):
            errors = self.parse_log_file(str(log_file))
            all_errors.extend(errors)
        
        return all_errors
    
    def group_errors_by_code(self, errors: List[LogError]) -> Dict[str, List[LogError]]:
        """
        오류 코드별로 그룹화
        
        Args:
            errors: 오류 정보 리스트
        
        Returns:
            오류 코드별 오류 리스트 딕셔너리
        """
        grouped = {}
        for error in errors:
            code = error.error_code
            if code not in grouped:
                grouped[code] = []
            grouped[code].append(error)
        
        return grouped
    
    def get_summary(self, errors: List[LogError]) -> Dict[str, Any]:
        """
        오류 요약 정보 생성
        
        Args:
            errors: 오류 정보 리스트
        
        Returns:
            요약 딕셔너리
        """
        summary = {
            "total_errors": len(errors),
            "by_type": {},
            "by_code": {},
            "affected_tables": set(),
            "affected_columns": set()
        }
        
        for error in errors:
            # 타입별 집계
            error_type = error.error_type
            if error_type not in summary["by_type"]:
                summary["by_type"][error_type] = 0
            summary["by_type"][error_type] += 1
            
            # 코드별 집계
            error_code = error.error_code
            if error_code not in summary["by_code"]:
                summary["by_code"][error_code] = 0
            summary["by_code"][error_code] += 1
            
            # 영향받은 테이블/컬럼
            if error.table_name:
                summary["affected_tables"].add(error.table_name)
            if error.column_name:
                summary["affected_columns"].add(error.column_name)
        
        summary["by_type"] = dict(summary["by_type"])
        summary["by_code"] = dict(summary["by_code"])
        summary["affected_tables"] = list(summary["affected_tables"])
        summary["affected_columns"] = list(summary["affected_columns"])
        
        return summary

