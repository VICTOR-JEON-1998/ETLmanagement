"""Job 메타데이터 파싱 모듈"""

import re
import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional, Set
from pathlib import Path

from src.core.logger import get_logger

logger = get_logger(__name__)


class JobParser:
    """DataStage Job 파서 클래스"""
    
    def __init__(self):
        """Job 파서 초기화"""
        pass
    
    def parse_job_definition(self, job_definition: Dict[str, Any]) -> Dict[str, Any]:
        """
        Job 정의에서 메타데이터 추출
        
        Args:
            job_definition: Job 정의 딕셔너리 (REST API 응답 또는 XML)
        
        Returns:
            파싱된 메타데이터 딕셔너리
        """
        metadata = {
            "job_name": None,
            "stages": [],
            "links": [],
            "source_tables": [],
            "target_tables": [],
            "columns": {}
        }
        
        # Job 이름 추출
        metadata["job_name"] = job_definition.get("name") or job_definition.get("jobName")
        
        # XML 형식인 경우
        if isinstance(job_definition, str) or "xml" in str(job_definition).lower():
            return self._parse_xml_job(job_definition, metadata)
        
        # JSON 형식인 경우
        return self._parse_json_job(job_definition, metadata)
    
    def _parse_xml_job(self, job_xml: Any, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        XML 형식 Job 파싱
        
        Args:
            job_xml: Job XML 문자열 또는 ElementTree
            metadata: 메타데이터 딕셔너리
        
        Returns:
            파싱된 메타데이터
        """
        try:
            if isinstance(job_xml, str):
                root = ET.fromstring(job_xml)
            else:
                root = job_xml
            
            # Stage 정보 추출
            stages = root.findall(".//stage")
            for stage in stages:
                stage_info = self._extract_stage_info(stage)
                if stage_info:
                    metadata["stages"].append(stage_info)
            
            # Link 정보 추출
            links = root.findall(".//link")
            for link in links:
                link_info = self._extract_link_info(link)
                if link_info:
                    metadata["links"].append(link_info)
            
            # 테이블 정보 추출
            metadata["source_tables"] = self._extract_tables(root, "source")
            metadata["target_tables"] = self._extract_tables(root, "target")
            
        except Exception as e:
            logger.error(f"XML Job 파싱 실패: {e}")
        
        return metadata
    
    def _parse_json_job(self, job_json: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        JSON 형식 Job 파싱
        
        Args:
            job_json: Job JSON 딕셔너리
            metadata: 메타데이터 딕셔너리
        
        Returns:
            파싱된 메타데이터
        """
        try:
            # Stage 정보 추출
            stages = job_json.get("stages", [])
            for stage in stages:
                stage_info = self._extract_stage_info_json(stage)
                if stage_info:
                    metadata["stages"].append(stage_info)
            
            # Link 정보 추출
            links = job_json.get("links", [])
            for link in links:
                link_info = self._extract_link_info_json(link)
                if link_info:
                    metadata["links"].append(link_info)
            
            # 테이블 정보 추출
            metadata["source_tables"] = self._extract_tables_json(job_json, "source")
            metadata["target_tables"] = self._extract_tables_json(job_json, "target")
            
        except Exception as e:
            logger.error(f"JSON Job 파싱 실패: {e}")
        
        return metadata
    
    def _extract_stage_info(self, stage: ET.Element) -> Optional[Dict[str, Any]]:
        """XML Stage 정보 추출"""
        try:
            return {
                "name": stage.get("name") or stage.findtext("name", ""),
                "type": stage.get("type") or stage.findtext("type", ""),
                "table_name": stage.findtext("tableName", ""),
                "schema": stage.findtext("schema", ""),
            }
        except Exception as e:
            logger.warning(f"Stage 정보 추출 실패: {e}")
            return None
    
    def _extract_stage_info_json(self, stage: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """JSON Stage 정보 추출"""
        try:
            return {
                "name": stage.get("name", ""),
                "type": stage.get("type", ""),
                "table_name": stage.get("tableName", ""),
                "schema": stage.get("schema", ""),
            }
        except Exception as e:
            logger.warning(f"Stage 정보 추출 실패: {e}")
            return None
    
    def _extract_link_info(self, link: ET.Element) -> Optional[Dict[str, Any]]:
        """XML Link 정보 추출"""
        try:
            columns = []
            for column in link.findall(".//column"):
                col_info = {
                    "name": column.get("name") or column.findtext("name", ""),
                    "type": column.get("type") or column.findtext("type", ""),
                    "length": int(column.get("length") or column.findtext("length", "0")),
                    "nullable": column.get("nullable", "false").lower() == "true"
                }
                columns.append(col_info)
            
            return {
                "name": link.get("name") or link.findtext("name", ""),
                "source_stage": link.get("sourceStage") or link.findtext("sourceStage", ""),
                "target_stage": link.get("targetStage") or link.findtext("targetStage", ""),
                "columns": columns
            }
        except Exception as e:
            logger.warning(f"Link 정보 추출 실패: {e}")
            return None
    
    def _extract_link_info_json(self, link: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """JSON Link 정보 추출"""
        try:
            columns = []
            for column in link.get("columns", []):
                col_info = {
                    "name": column.get("name", ""),
                    "type": column.get("type", ""),
                    "length": int(column.get("length", 0)),
                    "nullable": column.get("nullable", False)
                }
                columns.append(col_info)
            
            return {
                "name": link.get("name", ""),
                "source_stage": link.get("sourceStage", ""),
                "target_stage": link.get("targetStage", ""),
                "columns": columns
            }
        except Exception as e:
            logger.warning(f"Link 정보 추출 실패: {e}")
            return None
    
    def _extract_tables(self, root: ET.Element, table_type: str) -> List[Dict[str, Any]]:
        """
        테이블 정보 추출
        
        Args:
            root: XML 루트 요소
            table_type: "source" 또는 "target"
        
        Returns:
            테이블 정보 리스트
        """
        tables = []
        try:
            # Source/Target Stage에서 테이블 정보 추출
            stage_types = ["SequentialFile", "DataBase", "ODBC", "Oracle", "DB2"]
            if table_type == "source":
                stage_types.extend(["FileSet", "ComplexFlatFile"])
            
            for stage_type in stage_types:
                stages = root.findall(f".//stage[@type='{stage_type}']")
                for stage in stages:
                    table_name = stage.findtext("tableName", "")
                    schema = stage.findtext("schema", "")
                    if table_name:
                        tables.append({
                            "table_name": table_name,
                            "schema": schema,
                            "stage_name": stage.get("name", ""),
                            "stage_type": stage_type
                        })
        except Exception as e:
            logger.warning(f"테이블 정보 추출 실패: {e}")
        
        return tables
    
    def _extract_tables_json(self, job_json: Dict[str, Any], table_type: str) -> List[Dict[str, Any]]:
        """
        JSON에서 테이블 정보 추출
        
        Args:
            job_json: Job JSON 딕셔너리
            table_type: "source" 또는 "target"
        
        Returns:
            테이블 정보 리스트
        """
        tables = []
        try:
            stages = job_json.get("stages", [])
            for stage in stages:
                stage_type = stage.get("type", "")
                table_name = stage.get("tableName", "")
                
                if table_name and self._is_table_stage(stage_type, table_type):
                    tables.append({
                        "table_name": table_name,
                        "schema": stage.get("schema", ""),
                        "stage_name": stage.get("name", ""),
                        "stage_type": stage_type
                    })
        except Exception as e:
            logger.warning(f"테이블 정보 추출 실패: {e}")
        
        return tables
    
    def _is_table_stage(self, stage_type: str, table_type: str) -> bool:
        """Stage가 테이블 Stage인지 확인"""
        source_types = ["SequentialFile", "DataBase", "ODBC", "Oracle", "DB2", "FileSet"]
        target_types = ["SequentialFile", "DataBase", "ODBC", "Oracle", "DB2"]
        
        if table_type == "source":
            return stage_type in source_types
        else:
            return stage_type in target_types
    
    def extract_columns_from_job(self, job_definition: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Job에서 사용하는 모든 컬럼 정보 추출
        
        Args:
            job_definition: Job 정의
        
        Returns:
            테이블별 컬럼 정보 딕셔너리
        """
        metadata = self.parse_job_definition(job_definition)
        columns_by_table = {}
        
        # Link에서 컬럼 정보 추출
        for link in metadata.get("links", []):
            for column in link.get("columns", []):
                # 테이블 이름 추출 (Link의 source/target stage에서)
                # 실제 구현에서는 더 정교한 매핑 필요
                table_key = f"{link.get('source_stage')}_{link.get('target_stage')}"
                if table_key not in columns_by_table:
                    columns_by_table[table_key] = []
                
                columns_by_table[table_key].append({
                    "name": column.get("name"),
                    "type": column.get("type"),
                    "length": column.get("length"),
                    "nullable": column.get("nullable")
                })
        
        return columns_by_table

