"""DataStage DSX 파일 편집 모듈"""

import re
from typing import Dict, Any, List, Optional
from pathlib import Path
import copy

from src.core.logger import get_logger

logger = get_logger(__name__)


class DSXEditor:
    """DSX 파일 편집 클래스"""
    
    def __init__(self, dsx_file_path: str):
        """
        DSX 편집기 초기화
        
        Args:
            dsx_file_path: DSX 파일 경로
        """
        self.dsx_file_path = Path(dsx_file_path)
        self.content = None
        self.original_content = None
        self._load_file()
    
    def _load_file(self):
        """DSX 파일 로드"""
        try:
            with open(self.dsx_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                self.content = f.read()
                self.original_content = self.content
            logger.debug(f"DSX 파일 로드 성공: {self.dsx_file_path}")
        except Exception as e:
            logger.error(f"DSX 파일 로드 실패: {e}")
            raise
    
    def save(self, output_path: Optional[str] = None, backup: bool = True) -> bool:
        """
        수정된 DSX 파일 저장
        
        Args:
            output_path: 출력 파일 경로 (None이면 원본 파일 덮어쓰기)
            backup: 원본 파일 백업 여부
        
        Returns:
            저장 성공 여부
        """
        try:
            if output_path is None:
                output_path = self.dsx_file_path
            else:
                output_path = Path(output_path)
            
            # 백업 생성
            if backup and output_path == self.dsx_file_path:
                backup_path = output_path.with_suffix('.dsx.backup')
                with open(backup_path, 'w', encoding='utf-8', errors='ignore') as f:
                    f.write(self.original_content)
                logger.info(f"백업 파일 생성: {backup_path}")
            
            # 파일 저장
            with open(output_path, 'w', encoding='utf-8', errors='ignore') as f:
                f.write(self.content)
            
            logger.info(f"DSX 파일 저장 성공: {output_path}")
            return True
        except Exception as e:
            logger.error(f"DSX 파일 저장 실패: {e}")
            return False
    
    def replace_value(self, key: str, old_value: str, new_value: str, 
                     record_identifier: Optional[str] = None) -> int:
        """
        특정 키의 값 교체
        
        Args:
            key: 교체할 키 이름 (예: "TableName", "SchemaName")
            old_value: 기존 값
            new_value: 새로운 값
            record_identifier: 특정 DSRECORD Identifier (None이면 모든 레코드)
        
        Returns:
            교체된 횟수
        """
        count = 0
        pattern = rf'{key}\s+"{re.escape(old_value)}"'
        
        if record_identifier:
            # 특정 레코드 내에서만 교체
            record_pattern = rf'(BEGIN DSRECORD\s+Identifier\s+"{re.escape(record_identifier)}".*?END DSRECORD)'
            def replace_in_record(match):
                record_content = match.group(1)
                new_record = re.sub(pattern, f'{key} "{new_value}"', record_content)
                return new_record
            
            self.content = re.sub(record_pattern, replace_in_record, self.content, flags=re.DOTALL)
            count = len(re.findall(pattern, self.content))
        else:
            # 전체에서 교체
            matches = list(re.finditer(pattern, self.content))
            count = len(matches)
            for match in reversed(matches):  # 역순으로 교체 (인덱스 유지)
                start, end = match.span()
                self.content = self.content[:start] + f'{key} "{new_value}"' + self.content[end:]
        
        if count > 0:
            logger.info(f"값 교체 완료: {key} '{old_value}' → '{new_value}' ({count}회)")
        
        return count
    
    def replace_table_name(self, old_table: str, new_table: str, 
                          old_schema: Optional[str] = None, 
                          new_schema: Optional[str] = None) -> int:
        """
        테이블 이름 교체
        
        Args:
            old_table: 기존 테이블 이름
            new_table: 새로운 테이블 이름
            old_schema: 기존 스키마 이름 (선택)
            new_schema: 새로운 스키마 이름 (선택)
        
        Returns:
            교체된 횟수
        """
        count = 0
        
        # 스키마가 지정된 경우
        if old_schema and new_schema:
            # SchemaName과 TableName을 함께 교체
            pattern = rf'SchemaName\s+"{re.escape(old_schema)}".*?TableName\s+"{re.escape(old_table)}"'
            replacement = f'SchemaName "{new_schema}"\n                TableName "{new_table}"'
            matches = list(re.finditer(pattern, self.content, re.DOTALL))
            count = len(matches)
            for match in reversed(matches):
                start, end = match.span()
                self.content = self.content[:start] + replacement + self.content[end:]
        else:
            # 테이블 이름만 교체
            count = self.replace_value("TableName", old_table, new_table)
            if old_schema and new_schema:
                count += self.replace_value("SchemaName", old_schema, new_schema)
        
        if count > 0:
            logger.info(f"테이블 이름 교체 완료: {old_schema or ''}.{old_table} → {new_schema or ''}.{new_table} ({count}회)")
        
        return count
    
    def replace_connection_string(self, old_connection: str, new_connection: str) -> int:
        """
        연결 문자열 교체
        
        Args:
            old_connection: 기존 연결 문자열
            new_connection: 새로운 연결 문자열
        
        Returns:
            교체된 횟수
        """
        count = 0
        
        # 다양한 연결 문자열 키 시도
        connection_keys = ["ConnectionString", "DSN", "DatabaseName", "ServerName"]
        
        for key in connection_keys:
            pattern = rf'{key}\s+"{re.escape(old_connection)}"'
            matches = list(re.finditer(pattern, self.content))
            count += len(matches)
            for match in reversed(matches):
                start, end = match.span()
                self.content = self.content[:start] + f'{key} "{new_connection}"' + self.content[end:]
        
        if count > 0:
            logger.info(f"연결 문자열 교체 완료 ({count}회)")
        
        return count
    
    def update_job_name(self, new_name: str) -> bool:
        """
        Job 이름 변경
        
        Args:
            new_name: 새로운 Job 이름
        
        Returns:
            성공 여부
        """
        # ROOT 레코드의 Name 필드 변경
        pattern = r'(BEGIN DSRECORD\s+Identifier\s+"ROOT".*?Name\s+")[^"]+(".*?END DSRECORD)'
        
        def replace_name(match):
            return match.group(1) + new_name + match.group(2)
        
        new_content = re.sub(pattern, replace_name, self.content, flags=re.DOTALL)
        
        if new_content != self.content:
            self.content = new_content
            logger.info(f"Job 이름 변경 완료: → '{new_name}'")
            return True
        
        return False
    
    def update_description(self, new_description: str) -> bool:
        """
        Job 설명 변경
        
        Args:
            new_description: 새로운 설명
        
        Returns:
            성공 여부
        """
        # ROOT 레코드의 Description 필드 변경
        pattern = r'(BEGIN DSRECORD\s+Identifier\s+"ROOT".*?Description\s+")[^"]*(".*?END DSRECORD)'
        
        def replace_desc(match):
            return match.group(1) + new_description + match.group(2)
        
        new_content = re.sub(pattern, replace_desc, self.content, flags=re.DOTALL)
        
        if new_content != self.content:
            self.content = new_content
            logger.info(f"Job 설명 변경 완료")
            return True
        
        return False
    
    def get_job_name(self) -> Optional[str]:
        """Job 이름 조회"""
        pattern = r'BEGIN DSRECORD\s+Identifier\s+"ROOT".*?Name\s+"([^"]+)"'
        match = re.search(pattern, self.content, re.DOTALL)
        if match:
            return match.group(1)
        return None
    
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """
        DSX 파일의 모든 테이블 정보 추출
        
        Returns:
            테이블 정보 리스트
        """
        tables = []
        
        # DSRECORD에서 테이블 정보 찾기
        record_pattern = r'BEGIN DSRECORD\s+Identifier\s+"([^"]+)"(.*?)END DSRECORD'
        
        for match in re.finditer(record_pattern, self.content, re.DOTALL):
            identifier = match.group(1)
            record_content = match.group(2)
            
            # Database 관련 Stage 확인
            olet_type = self._extract_value(record_content, "OLEType")
            if olet_type and any(x in olet_type for x in ["Database", "ODBC", "Oracle", "DB2", "Table"]):
                table_name = self._extract_value(record_content, "TableName")
                schema = self._extract_value(record_content, "SchemaName")
                stage_name = self._extract_value(record_content, "Name")
                
                if table_name:
                    tables.append({
                        "identifier": identifier,
                        "stage_name": stage_name or identifier,
                        "table_name": table_name,
                        "schema": schema or "",
                        "type": olet_type
                    })
        
        return tables
    
    def _extract_value(self, content: str, key: str) -> Optional[str]:
        """레코드 내용에서 키 값 추출"""
        pattern = rf'{key}\s+"([^"]+)"'
        match = re.search(pattern, content)
        if match:
            return match.group(1)
        return None
    
    def get_changes_summary(self) -> Dict[str, Any]:
        """
        변경 사항 요약
        
        Returns:
            변경 사항 딕셔너리
        """
        return {
            "original_size": len(self.original_content),
            "current_size": len(self.content),
            "has_changes": self.content != self.original_content,
            "job_name": self.get_job_name()
        }

