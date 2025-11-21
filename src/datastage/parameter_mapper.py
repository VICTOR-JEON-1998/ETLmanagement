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
        
        # ERP 테이블의 경우: #P_ERP_MS.$P_ERP_MS_OWN_FILA_ERP#.dbo.IF_DW_CART_M
        # 형식에서 마지막 두 점 사이가 스키마(dbo), 마지막 점 이후가 테이블명
        # Vertica 테이블의 경우: #P_DW_VER.$P_DW_VER_OWN_BIDWADM#.FT_AS_ACCP_RSLT
        # 형식에서 마지막 점 이후가 테이블명
        
        # ERP인지 먼저 확인
        is_erp = "ERP" in param_table_name.upper()
        
        if is_erp:
            # ERP 테이블: 파라미터 부분이 #로 끝나고, 그 이후에 스키마.테이블명이 올 수 있음
            # 형식: #P_ERP_MS.$P_ERP_MS_OWN_FILA_ERP#.dbo.IF_DW_CART_M 또는
            #       #P_ERP_MS.$P_ERP_MS_OWN_FILA_ERP#.DW_ETL_L
            # 파라미터 부분이 끝나는 지점 (#) 찾기
            param_end_idx = param_table_name.rfind("#")
            if param_end_idx >= 0 and param_end_idx < len(param_table_name) - 1:
                # 파라미터 부분 이후의 문자열
                after_param = param_table_name[param_end_idx + 1:]
                param_part = param_table_name[:param_end_idx + 1]  # #P_ERP_MS.$P_ERP_MS_OWN_FILA_ERP#
                
                # 파라미터 이후 부분에서 점으로 분리
                if after_param.startswith("."):
                    after_param = after_param[1:]  # 앞의 점 제거
                
                if after_param:
                    # 점이 있으면 스키마.테이블명, 없으면 테이블명만
                    after_parts = after_param.split(".", 1)
                    if len(after_parts) == 2:
                        # 스키마.테이블명 형식
                        schema = after_parts[0]  # dbo
                        table_name = after_parts[1]  # IF_DW_CART_M
                    else:
                        # 테이블명만 있는 경우, 기본 스키마 dbo 사용
                        schema = "dbo"
                        table_name = after_parts[0]  # DW_ETL_L
                else:
                    table_name = ""
                    schema = ""
            else:
                # #가 없는 경우 (이상한 형식)
                param_part = param_table_name
                table_name = ""
                schema = ""
        else:
            # Vertica 테이블: 마지막 점 이후가 테이블명
            parts = param_table_name.rsplit(".", 1)
            if len(parts) == 2:
                param_part = parts[0]  # #P_DW_VER.$P_DW_VER_OWN_BIDWADM#
                table_name = parts[1]  # FT_AS_ACCP_RSLT
            else:
                param_part = param_table_name
                table_name = ""
            schema = ""
        
        # 파라미터에서 DB 타입 판단
        db_type = None
        
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
            # ERP의 경우 위에서 이미 스키마를 추출했거나, 기본값 dbo 사용
            # 스키마가 아직 설정되지 않았으면 기본값 dbo 사용
            if not schema:
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

