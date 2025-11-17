"""DataStage 파라미터 매핑 모듈"""

import re
from typing import Dict, Any, Optional, List
from src.core.logger import get_logger

logger = get_logger(__name__)


class ParameterMapper:
    """DataStage 파라미터를 실제 DB 정보로 매핑하는 클래스"""
    
    def parse_parameter_table(self, param_table_name: str) -> Dict[str, Any]:
        """
        파라미터 형식의 테이블명을 파싱
        
        Args:
            param_table_name: 파라미터 형식의 테이블명 (예: #P_DW_VER.$P_DW_VER_OWN_BIDWADM#.FT_AS_ACCP_RSLT)
        
        Returns:
            파싱된 정보 딕셔너리:
            {
                "db_type": "vertica" | "mssql" | None,
                "schema": "스키마명",
                "table_name": "테이블명",
                "original": "원본 파라미터",
                "is_parameter": True
            }
        """
        if not param_table_name or not param_table_name.startswith("#"):
            return {
                "db_type": None,
                "schema": "",
                "table_name": param_table_name,
                "original": param_table_name,
                "is_parameter": False
            }
        
        original = param_table_name
        
        # 마지막 점(.) 이후가 테이블명
        parts = param_table_name.rsplit(".", 1)
        if len(parts) == 2:
            param_part = parts[0]  # #P_DW_VER.$P_DW_VER_OWN_BIDWADM#
            table_name = parts[1]  # FT_AS_ACCP_RSLT
        else:
            # 점이 없는 경우 (파라미터만)
            param_part = param_table_name
            table_name = ""
        
        # 파라미터에서 DB 타입 판단
        db_type = None
        schema = ""
        
        # BIDW가 포함되어 있으면 Vertica
        if "BIDW" in param_part.upper():
            db_type = "vertica"
            # 스키마 추출: $P_DW_VER_OWN_BIDWADM에서 OWN_ 뒤의 값
            schema_match = re.search(r'\$P_[^#]*OWN_([^#]+)', param_part)
            if schema_match:
                schema = schema_match.group(1)
        # ERP가 포함되어 있으면 MSSQL
        elif "ERP" in param_part.upper():
            db_type = "mssql"
            # MSSQL은 보통 스키마가 없거나 dbo
            schema = "dbo"
        
        return {
            "db_type": db_type,
            "schema": schema,
            "table_name": table_name,
            "original": original,
            "is_parameter": True
        }
    
    def resolve_table_info(self, param_table_name: str) -> Dict[str, Any]:
        """
        파라미터 테이블명을 실제 DB 정보로 변환
        
        Args:
            param_table_name: 파라미터 형식의 테이블명
        
        Returns:
            해석된 테이블 정보
        """
        parsed = self.parse_parameter_table(param_table_name)
        
        result = {
            "db_type": parsed["db_type"],
            "schema": parsed["schema"],
            "table_name": parsed["table_name"],
            "full_name": f"{parsed['schema']}.{parsed['table_name']}" if parsed['schema'] else parsed['table_name'],
            "original": parsed["original"],
            "is_parameter": parsed["is_parameter"]
        }
        
        return result
    
    def map_tables(self, tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        테이블 리스트의 파라미터를 실제 DB 정보로 매핑
        
        Args:
            tables: 테이블 정보 리스트
        
        Returns:
            매핑된 테이블 정보 리스트
        """
        mapped_tables = []
        
        for table in tables:
            table_name = table.get("table_name", "")
            schema = table.get("schema", "")
            
            # 파라미터 형식인 경우 매핑
            if table_name.startswith("#"):
                resolved = self.resolve_table_info(table_name)
                
                mapped_table = {
                    **table,
                    "table_name": resolved["table_name"],
                    "schema": resolved["schema"],
                    "db_type": resolved["db_type"],
                    "full_name": resolved["full_name"],
                    "original_parameter": resolved["original"],
                    "is_parameter": True
                }
            else:
                # 파라미터가 아닌 경우 그대로 사용
                mapped_table = {
                    **table,
                    "is_parameter": False
                }
            
            mapped_tables.append(mapped_table)
        
        return mapped_tables

