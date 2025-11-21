"""DataStage Export 파일(.dsx) 파서 모듈"""

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from src.core.logger import get_logger

logger = get_logger(__name__)


class DSXParser:
    """DataStage Export 파일(.dsx) 파서 클래스"""
    
    def __init__(self):
        """DSX 파서 초기화"""
        pass
    
    def parse_dsx_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        DSX 파일 파싱
        
        Args:
            file_path: DSX 파일 경로
        
        Returns:
            파싱된 Job 정보 딕셔너리
        """
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            return self.parse_dsx_content(content, file_path)
        except Exception as e:
            logger.error(f"DSX 파일 읽기 실패: {file_path} - {e}")
            return None
    
    def parse_dsx_content(self, content: str, file_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        DSX 파일 내용 파싱 (단일 Job 또는 첫 번째 Job)
        
        Args:
            content: DSX 파일 내용
            file_path: 파일 경로 (선택)
        
        Returns:
            파싱된 Job 정보 딕셔너리
        """
        try:
            job_info = {
                "name": None,
                "identifier": None,
                "description": None,
                "category": None,
                "date_modified": None,
                "time_modified": None,
                "server_name": None,
                "project": None,
                "file_path": file_path,
                "stages": [],
                "source_tables": [],
                "target_tables": []
            }
            
            # HEADER 섹션 파싱
            header_match = re.search(r'BEGIN HEADER\s+(.*?)\s+END HEADER', content, re.DOTALL)
            if header_match:
                header_content = header_match.group(1)
                job_info["server_name"] = self._extract_value(header_content, "ServerName")
                job_info["project"] = self._extract_value(header_content, "ToolInstanceID")
            
            # DSJOB 섹션 파싱 (첫 번째 Job)
            dsjob_match = re.search(r'BEGIN DSJOB\s+(.*?)\s+END DSJOB', content, re.DOTALL)
            if dsjob_match:
                dsjob_content = dsjob_match.group(1)
                job_info["identifier"] = self._extract_value(dsjob_content, "Identifier")
                job_info["date_modified"] = self._extract_value(dsjob_content, "DateModified")
                job_info["time_modified"] = self._extract_value(dsjob_content, "TimeModified")
            
            # DSRECORD 섹션에서 Job 정보 추출 (첫 번째 ROOT)
            dsrecord_match = re.search(r'BEGIN DSRECORD\s+Identifier\s+"ROOT"(.*?)END DSRECORD', content, re.DOTALL)
            if dsrecord_match:
                record_content = dsrecord_match.group(1)
                job_info["name"] = self._extract_value(record_content, "Name") or job_info["identifier"]
                job_info["description"] = self._extract_value(record_content, "Description")
                job_info["category"] = self._extract_value(record_content, "Category")
            
            # Stage 정보 추출
            job_info["stages"] = self._extract_stages(content)
            
            # 테이블 정보 추출
            job_info["source_tables"] = self._extract_tables(content, "source")
            job_info["target_tables"] = self._extract_tables(content, "target")
            
            return job_info
            
        except Exception as e:
            logger.error(f"DSX 내용 파싱 실패: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    def parse_multiple_jobs(self, content: str, file_path: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        DSX 파일에서 여러 Job 파싱 (하나의 파일에 여러 Job이 포함된 경우)
        
        Args:
            content: DSX 파일 내용
            file_path: 파일 경로 (선택)
        
        Returns:
            파싱된 Job 정보 리스트
        """
        jobs = []
        try:
            # HEADER 섹션 파싱 (전체 파일에 하나)
            header_match = re.search(r'BEGIN HEADER\s+(.*?)\s+END HEADER', content, re.DOTALL)
            server_name = None
            project = None
            if header_match:
                header_content = header_match.group(1)
                server_name = self._extract_value(header_content, "ServerName")
                project = self._extract_value(header_content, "ToolInstanceID")
            
            # 여러 DSJOB 섹션 찾기
            dsjob_pattern = r'BEGIN DSJOB\s+(.*?)\s+END DSJOB'
            dsjob_matches = list(re.finditer(dsjob_pattern, content, re.DOTALL))
            
            if not dsjob_matches:
                # DSJOB 섹션이 없으면 단일 Job으로 처리
                job_info = self.parse_dsx_content(content, file_path)
                if job_info:
                    jobs.append(job_info)
                return jobs
            
            # 각 DSJOB 섹션별로 Job 파싱
            for i, dsjob_match in enumerate(dsjob_matches):
                try:
                    dsjob_start = dsjob_match.start()
                    dsjob_end = dsjob_match.end()
                    
                    # 다음 DSJOB까지 또는 파일 끝까지의 범위
                    if i + 1 < len(dsjob_matches):
                        next_dsjob_start = dsjob_matches[i + 1].start()
                        job_content = content[dsjob_start:next_dsjob_start]
                    else:
                        job_content = content[dsjob_start:]
                    
                    # DSJOB 정보 추출
                    dsjob_content = dsjob_match.group(1)
                    identifier = self._extract_value(dsjob_content, "Identifier")
                    date_modified = self._extract_value(dsjob_content, "DateModified")
                    time_modified = self._extract_value(dsjob_content, "TimeModified")
                    
                    # 이 Job의 ROOT DSRECORD 찾기
                    # ROOT는 보통 DSJOB 바로 다음에 위치
                    root_pattern = rf'BEGIN DSRECORD\s+Identifier\s+"ROOT"(.*?)END DSRECORD'
                    root_match = re.search(root_pattern, job_content, re.DOTALL)
                    
                    job_name = identifier
                    description = None
                    category = None
                    
                    if root_match:
                        record_content = root_match.group(1)
                        job_name = self._extract_value(record_content, "Name") or identifier
                        description = self._extract_value(record_content, "Description")
                        category = self._extract_value(record_content, "Category")
                    
                    # 이 Job의 Stage와 테이블 정보 추출
                    stages = self._extract_stages(job_content)
                    
                    # 한 번에 모든 테이블 추출
                    tables_result = self._extract_all_tables(job_content)
                    source_tables = tables_result["source_tables"]
                    target_tables = tables_result["target_tables"]
                    
                    job_info = {
                        "name": job_name,
                        "identifier": identifier,
                        "description": description,
                        "category": category,
                        "date_modified": date_modified,
                        "time_modified": time_modified,
                        "server_name": server_name,
                        "project": project,
                        "file_path": file_path,
                        "stages": stages,
                        "source_tables": source_tables,
                        "target_tables": target_tables
                    }
                    
                    if job_name:
                        jobs.append(job_info)
                        logger.debug(f"Job 파싱 성공: {job_name} (파일: {Path(file_path).name if file_path else 'N/A'})")
                
                except Exception as e:
                    logger.debug(f"Job {i+1} 파싱 실패: {e}")
                    continue
            
            logger.info(f"DSX 파일에서 {len(jobs)}개 Job 파싱 완료: {Path(file_path).name if file_path else 'N/A'}")
            return jobs
            
        except Exception as e:
            logger.error(f"다중 Job 파싱 실패: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []
    
    def _extract_value(self, content: str, key: str) -> Optional[str]:
        """DSX 내용에서 키 값 추출 (개선된 버전 - 여러 줄 값 지원)"""
        # 패턴 1: 일반적인 형식: Key "value"
        pattern1 = rf'{key}\s+"([^"]+)"'
        match = re.search(pattern1, content)
        if match:
            return match.group(1)
        
        # 패턴 2: 여러 줄에 걸친 값 (Value =+=+=+= ... =+=+=+=)
        # XMLProperties 같은 경우
        pattern2 = rf'{key}\s+Value\s+(?:=+=+=+=)?\s*(.*?)\s*(?:=+=+=+=)?\s+END DSSUBRECORD'
        match = re.search(pattern2, content, re.DOTALL)
        if match:
            value = match.group(1).strip()
            # =+=+=+= 제거
            if value.startswith("=+=+=+="):
                value = value[7:].strip()
            if value.endswith("=+=+=+="):
                value = value[:-7].strip()
            return value
        
        return None
    
    def _extract_stages(self, content: str) -> List[Dict[str, Any]]:
        """Stage 정보 추출"""
        stages = []
        try:
            # DSRECORD에서 Stage 찾기
            stage_pattern = r'BEGIN DSRECORD\s+Identifier\s+"([^"]+)"(.*?)END DSRECORD'
            for match in re.finditer(stage_pattern, content, re.DOTALL):
                identifier = match.group(1)
                record_content = match.group(2)
                
                # Stage 타입 확인
                olet_type = self._extract_value(record_content, "OLEType")
                if olet_type and "Stage" in olet_type:
                    stage_name = self._extract_value(record_content, "Name") or identifier
                    stages.append({
                        "identifier": identifier,
                        "name": stage_name,
                        "type": olet_type,
                        "description": self._extract_value(record_content, "Description")
                    })
        except Exception as e:
            logger.debug(f"Stage 추출 중 오류: {e}")
        
        return stages
    
    def _extract_tables(self, content: str, table_type: str) -> List[Dict[str, Any]]:
        """테이블 정보 추출 (개선된 버전 - XMLProperties 지원)"""
        tables = []
        try:
            import xml.etree.ElementTree as ET
            
            # 모든 DSRECORD 찾기
            stage_pattern = r'BEGIN DSRECORD\s+Identifier\s+"([^"]+)"(.*?)END DSRECORD'
            for match in re.finditer(stage_pattern, content, re.DOTALL):
                identifier = match.group(1)
                record_content = match.group(2)
                
                olet_type = self._extract_value(record_content, "OLEType")
                stage_name = self._extract_value(record_content, "Name") or identifier
                stage_type = self._extract_value(record_content, "StageType") or ""
                
                # CCustomStage, CCustomInput, CCustomOutput 등 확인
                is_custom_stage = olet_type and ("CCustom" in olet_type or "Stage" in olet_type)
                is_connector = stage_type and ("Connector" in stage_type or "ODBC" in stage_type)
                
                table_name = None
                schema = None
                
                # 방법 0: TableDef에서 ERP 테이블 먼저 찾기 (가장 우선, XMLProperties 전)
                if table_type == "source":
                    tabledef_match = re.search(r'TableDef\s+"([^"]+)"', record_content)
                    if tabledef_match:
                        tabledef_value = tabledef_match.group(1)
                        is_erp_in_tabledef = "FILA_ERP" in tabledef_value or ("ERP" in tabledef_value.upper() and "FILA" in tabledef_value)
                        
                        if is_erp_in_tabledef:
                            # 경로에서 마지막 부분 추출
                            parts = tabledef_value.split("\\")
                            if len(parts) >= 2:
                                last_part = parts[-1]
                                if "." in last_part:
                                    schema_table = last_part.split(".", 1)
                                    if len(schema_table) == 2:
                                        temp_schema = schema_table[0]
                                        temp_table = schema_table[1]
                                        
                                        # dbo는 스키마가 아니므로 제거
                                        if temp_schema.lower() == "dbo":
                                            # FILA_ERP.dbo.DW_ETL_L -> FILA_ERP.DW_ETL_L
                                            for p in parts:
                                                if "FILA_ERP" in p:
                                                    if "FILA_ERP_DW" in p:
                                                        temp_schema = "FILA_ERP_DW"
                                                    else:
                                                        temp_schema = "FILA_ERP"
                                                    break
                                        
                                        if temp_schema and temp_table:
                                            # 파라미터 형식으로 변환하여 저장
                                            table_name_param = f"#P_ERP_MS.$P_ERP_MS_OWN_{temp_schema}#.{temp_table}"
                                            # 바로 추가하고 다음 레코드로
                                            tables.append({
                                                "table_name": table_name_param,
                                                "schema": "",
                                                "stage_name": stage_name,
                                                "stage_type": olet_type or stage_type or "Unknown",
                                                "table_type": table_type
                                            })
                                            # 다음 레코드로 이동
                                            continue
                
                # Context 변수 초기화 (먼저 XMLProperties에서 Context 확인)
                context_value = None
                xml_properties = self._extract_value(record_content, "XMLProperties")
                
                # 먼저 XMLProperties에서 Context 확인 (방법 1에서 찾은 테이블도 필터링하기 위해)
                if xml_properties:
                    # =+=+=+= 로 감싸진 경우 처리
                    xml_props_for_context = xml_properties
                    if xml_props_for_context.startswith("=+=+=+="):
                        xml_props_for_context = xml_props_for_context[7:].strip()
                    if xml_props_for_context.endswith("=+=+=+="):
                        xml_props_for_context = xml_props_for_context[:-7].strip()
                    
                    try:
                        root_context = ET.fromstring(xml_props_for_context)
                        context_elem = root_context.find(".//Context")
                        if context_elem is not None and context_elem.text:
                            context_value = context_elem.text.strip()
                    except:
                        pass
                
                # 방법 1: 직접 TableName 필드 찾기 (기존 방식)
                table_name = self._extract_value(record_content, "TableName")
                schema = self._extract_value(record_content, "SchemaName")
                
                # 방법 1에서 테이블을 찾은 경우, 방법 2는 건너뛰기 (중복 방지)
                method1_table_found = table_name is not None
                
                # 방법 2: XMLProperties에서 TableName 추출 및 Context 확인
                if xml_properties and not method1_table_found:
                    # =+=+=+= 로 감싸진 경우 처리
                    if xml_properties.startswith("=+=+=+="):
                        xml_properties = xml_properties[7:].strip()
                    if xml_properties.endswith("=+=+=+="):
                        xml_properties = xml_properties[:-7].strip()
                    
                    try:
                        # XML 파싱
                        # ElementTree는 CDATA를 자동으로 처리하므로 text 속성에서 바로 값을 가져올 수 있음
                        root = ET.fromstring(xml_properties)
                        
                        # Context 확인 (source/target 구분)
                        # Context는 XMLProperties 안에 숫자로 저장됨: 1 = source, 2 = target
                        # 이미 위에서 확인했지만, 다시 확인 (혹시 모를 경우를 위해)
                        if not context_value:
                            context_elem = root.find(".//Context")
                            if context_elem is not None and context_elem.text:
                                context_value = context_elem.text.strip()
                        
                        # 먼저 TableDef에서 ERP 테이블 찾기 (Context 필터링 전, 가장 우선)
                        # TableDef는 DSRECORD 레벨에 있으므로 record_content에서 찾기
                        tabledef_match = re.search(r'TableDef\s+"([^"]+)"', record_content)
                        if tabledef_match and table_type == "source":
                            tabledef_value = tabledef_match.group(1)
                            is_erp_in_tabledef = "FILA_ERP" in tabledef_value or ("ERP" in tabledef_value.upper() and "FILA" in tabledef_value)
                            
                            if is_erp_in_tabledef:
                                # 경로에서 마지막 부분 추출
                                parts = tabledef_value.split("\\")
                                if len(parts) >= 2:
                                    last_part = parts[-1]
                                    if "." in last_part:
                                        schema_table = last_part.split(".", 1)
                                        if len(schema_table) == 2:
                                            temp_schema = schema_table[0]
                                            temp_table = schema_table[1]
                                            
                                            # dbo는 스키마가 아니므로 제거
                                            if temp_schema.lower() == "dbo":
                                                # FILA_ERP.dbo.DW_ETL_L -> FILA_ERP.DW_ETL_L
                                                for p in parts:
                                                    if "FILA_ERP" in p:
                                                        # FILA_ERP 또는 FILA_ERP_DW 추출
                                                        if "FILA_ERP_DW" in p:
                                                            temp_schema = "FILA_ERP_DW"
                                                        else:
                                                            temp_schema = "FILA_ERP"
                                                        break
                                            
                                            if temp_schema and temp_table:
                                                # 파라미터 형식으로 변환하여 저장
                                                # FILA_ERP.DW_ETL_L -> #P_ERP_MS.$P_ERP_MS_OWN_FILA_ERP#.DW_ETL_L
                                                table_name = f"#P_ERP_MS.$P_ERP_MS_OWN_{temp_schema}#.{temp_table}"
                                                schema = ""
                                                # ERP 테이블은 보통 소스이므로 Context 필터링 없이 바로 추가하지 않고
                                                # table_name을 설정하고 나중에 Context 필터링을 거치도록 함
                                                # (다음 레코드로 이동하지 않고 계속 진행하여 Context 필터링을 거침)
                                                break
                        
                        # 먼저 SelectStatement에서 ERP 테이블 찾기 (Context 필터링 전)
                        # ERP 테이블은 보통 소스이므로 소스 타입일 때만 찾기
                        if table_type == "source" and not table_name:
                            for sql_elem in root.findall(".//SelectStatement"):
                                if sql_elem.text:
                                    sql_text = sql_elem.text.strip()
                                    # FROM 절에서 ERP 테이블 추출
                                    from_matches = re.findall(r'FROM\s+([^\s,;]+(?:\.[^\s,;]+)*)', sql_text, re.IGNORECASE | re.DOTALL)
                                    for from_match in from_matches:
                                        table_ref = from_match.strip()
                                        table_ref = re.sub(r'\s+', '', table_ref)
                                        
                                        # ERP 테이블인지 확인
                                        if "#P_ERP" in table_ref or ("ERP" in table_ref.upper() and "." in table_ref):
                                            # ERP 테이블 발견 - 파라미터 형식 처리
                                            if "." in table_ref:
                                                parts = table_ref.rsplit(".", 1)
                                                if len(parts) == 2:
                                                    if parts[0].startswith("#"):
                                                        table_name = table_ref
                                                    else:
                                                        table_name = parts[1]
                                                        schema = parts[0]
                                                else:
                                                    table_name = table_ref
                                            else:
                                                table_name = table_ref
                                            
                                            # ERP 테이블을 찾았으면 table_name을 설정하고 계속 진행
                                            # (Context 필터링을 거치도록 함)
                                            if table_name and table_name.strip() and not table_name.endswith("#."):
                                                break
                                    if table_name:  # ERP 테이블을 찾은 경우
                                        break
                        
                        # ERP 테이블인지 확인 (TableName 기준)
                        is_erp_table = False
                        temp_table_name = None
                        for table_elem in root.findall(".//TableName"):
                            if table_elem.text:
                                temp_table_name = table_elem.text.strip()
                                if "#P_ERP" in temp_table_name or "ERP" in temp_table_name.upper():
                                    is_erp_table = True
                                break
                        
                        # TableName 찾기 (Context 확인 전에 먼저 찾기)
                        if not table_name:  # SelectStatement에서 찾지 못한 경우만
                            for table_elem in root.findall(".//TableName"):
                                if table_elem.text:
                                    table_name = table_elem.text.strip()
                                    break
                        
                        # 방법 2에서는 테이블만 찾고, Context 필터링은 마지막에 한 번만 적용
                        
                        # SchemaName 찾기 (있는 경우)
                        if not schema:
                            for schema_elem in root.findall(".//SchemaName"):
                                if schema_elem.text:
                                    schema = schema_elem.text.strip()
                                    break
                        
                        # SQL 문에서 테이블 추출 (TableName이 없는 경우, ERP가 아닌 경우)
                        if not table_name:
                            # SelectStatement 찾기
                            for sql_elem in root.findall(".//SelectStatement"):
                                if sql_elem.text:
                                    sql_text = sql_elem.text.strip()
                                    # FROM 절에서 테이블 추출
                                    # FROM #P_ERP_MS.$P_ERP_MS_OWN_FILA_ERP#.WM_WRHS_M 같은 패턴
                                    # 여러 줄에 걸쳐 있을 수 있으므로 DOTALL 사용
                                    # 여러 테이블이 있을 수 있으므로 모두 찾기
                                    from_matches = re.findall(r'FROM\s+([^\s,;]+(?:\.[^\s,;]+)*)', sql_text, re.IGNORECASE | re.DOTALL)
                                    for from_match in from_matches:
                                        table_ref = from_match.strip()
                                        # 개행 문자 제거
                                        table_ref = re.sub(r'\s+', '', table_ref)
                                        
                                        # ERP 테이블인지 확인
                                        is_erp_in_sql = "#P_ERP" in table_ref or "ERP" in table_ref.upper()
                                        
                                        # ERP 테이블이고 소스 타입인 경우만 처리
                                        if is_erp_in_sql and table_type == "source":
                                            # 파라미터 형식: #P_ERP_MS.$P_ERP_MS_OWN_FILA_ERP#.WM_WRHS_M
                                            # 또는 일반 스키마.테이블 형식
                                            if "." in table_ref:
                                                parts = table_ref.rsplit(".", 1)
                                                if len(parts) == 2:
                                                    # 스키마 부분이 파라미터인 경우 전체를 table_name으로 저장
                                                    if parts[0].startswith("#"):
                                                        # 파라미터 전체를 table_name으로 저장 (나중에 파라미터 매퍼가 해석)
                                                        table_name = table_ref
                                                    else:
                                                        # 일반 스키마.테이블 형식
                                                        table_name = parts[1]
                                                        schema = parts[0]
                                            else:
                                                table_name = table_ref
                                            break
                                        elif not is_erp_in_sql:
                                            # ERP가 아닌 경우 기존 로직 사용
                                            if "." in table_ref:
                                                parts = table_ref.rsplit(".", 1)
                                                if len(parts) == 2:
                                                    if parts[0].startswith("#"):
                                                        table_name = table_ref
                                                    else:
                                                        table_name = parts[1]
                                                        schema = parts[0]
                                            else:
                                                table_name = table_ref
                                            break
                                    if table_name:
                                        break
                            
                            # SQL 필드도 확인
                            if not table_name:
                                for sql_elem in root.findall(".//SQL"):
                                    if sql_elem.text:
                                        sql_text = sql_elem.text.strip()
                                        from_match = re.search(r'FROM\s+([^\s,;]+(?:\.[^\s,;]+)*)', sql_text, re.IGNORECASE | re.DOTALL)
                                        if from_match:
                                            table_ref = from_match.group(1).strip()
                                            table_ref = re.sub(r'\s+', '', table_ref)
                                            if "." in table_ref:
                                                parts = table_ref.rsplit(".", 1)
                                                if len(parts) == 2:
                                                    # 스키마 부분이 파라미터인 경우 전체를 table_name으로 저장
                                                    if parts[0].startswith("#"):
                                                        table_name = table_ref
                                                    else:
                                                        table_name = parts[1]
                                                        schema = parts[0]
                                            else:
                                                table_name = table_ref
                                            break
                                if table_name:
                                    break
                    
                    except (ET.ParseError, UnicodeDecodeError) as e:
                            logger.debug(f"XML 파싱 실패: {e}")
                            # XML 파싱 실패 시 정규식으로 시도
                            # CDATA 안의 내용은 여러 줄일 수 있고 ]가 포함될 수 있으므로 더 정확한 패턴 사용
                            table_match = re.search(r'<TableName[^>]*><!\[CDATA\[(.*?)\]\]></TableName>', xml_properties, re.DOTALL)
                            if table_match:
                                table_name = table_match.group(1).strip()
                            
                            schema_match = re.search(r'<SchemaName[^>]*><!\[CDATA\[(.*?)\]\]></SchemaName>', xml_properties, re.DOTALL)
                            if schema_match:
                                schema = schema_match.group(1).strip()
                            
                            # SQL 문에서 테이블 추출 (XML 파싱 실패 시)
                            if not table_name:
                                # SelectStatement CDATA 찾기
                                select_match = re.search(r'<SelectStatement[^>]*><!\[CDATA\[(.*?)\]\]></SelectStatement>', xml_properties, re.DOTALL)
                                if select_match:
                                    sql_text = select_match.group(1).strip()
                                    from_match = re.search(r'FROM\s+([^\s,;]+(?:\.[^\s,;]+)*)', sql_text, re.IGNORECASE | re.DOTALL)
                                    if from_match:
                                        table_ref = from_match.group(1).strip()
                                        table_ref = re.sub(r'\s+', '', table_ref)
                                        if "." in table_ref:
                                            parts = table_ref.rsplit(".", 1)
                                            if len(parts) == 2:
                                                # 스키마 부분이 파라미터인 경우 전체를 table_name으로 저장
                                                if parts[0].startswith("#"):
                                                    table_name = table_ref
                                                else:
                                                    table_name = parts[1]
                                                    schema = parts[0]
                                        else:
                                            table_name = table_ref
                                
                                # SQL 필드도 확인
                                if not table_name:
                                    sql_match = re.search(r'<SQL[^>]*><!\[CDATA\[(.*?)\]\]></SQL>', xml_properties, re.DOTALL)
                                    if sql_match:
                                        sql_text = sql_match.group(1).strip()
                                        from_match = re.search(r'FROM\s+([^\s,;]+(?:\.[^\s,;]+)*)', sql_text, re.IGNORECASE | re.DOTALL)
                                        if from_match:
                                            table_ref = from_match.group(1).strip()
                                            table_ref = re.sub(r'\s+', '', table_ref)
                                            if "." in table_ref:
                                                parts = table_ref.rsplit(".", 1)
                                                if len(parts) == 2:
                                                    # 스키마 부분이 파라미터인 경우 전체를 table_name으로 저장
                                                    if parts[0].startswith("#"):
                                                        table_name = table_ref
                                                    else:
                                                        table_name = parts[1]
                                                        schema = parts[0]
                                            else:
                                                table_name = table_ref
                
                # 방법 3: TableDef 필드에서 테이블 추출 (ERP 테이블 등)
                if not table_name:
                    # TableDef "ODBC\\SQLServer_dev_FILA_ERP\\FILA_ERP.dbo.DW_ETL_L"
                    # TableDef "Database\\ERPDEV2\\BIDWADM.CD_DAY_NM"
                    tabledef_match = re.search(r'TableDef\s+"([^"]+)"', record_content)
                    if tabledef_match:
                        tabledef_value = tabledef_match.group(1)
                        
                        # ERP 관련 확인
                        is_erp_in_tabledef = "FILA_ERP" in tabledef_value or ("ERP" in tabledef_value.upper() and "FILA" in tabledef_value)
                        
                        # ERP 테이블이고 소스 타입인 경우만 처리
                        if is_erp_in_tabledef and table_type == "source":
                            # 경로에서 마지막 부분 추출
                            # ODBC\\SQLServer_dev_FILA_ERP\\FILA_ERP.dbo.DW_ETL_L
                            # Database\\ERPDEV2\\BIDWADM.CD_DAY_NM
                            parts = tabledef_value.split("\\")
                            if len(parts) >= 2:
                                last_part = parts[-1]
                                if "." in last_part:
                                    schema_table = last_part.split(".", 1)
                                    if len(schema_table) == 2:
                                        schema = schema_table[0]
                                        table = schema_table[1]
                                        
                                        # dbo는 스키마가 아니므로 제거
                                        if schema.lower() == "dbo":
                                            # FILA_ERP.dbo.DW_ETL_L -> FILA_ERP.DW_ETL_L
                                            # 이전 부분에서 스키마 찾기
                                            if len(parts) >= 2:
                                                prev_part = parts[-2]
                                                if "FILA_ERP" in prev_part:
                                                    # FILA_ERP 관련 부분 찾기
                                                    for p in parts:
                                                        if "FILA_ERP" in p:
                                                            schema = "FILA_ERP"
                                                            break
                                            else:
                                                schema = ""
                                        
                                        # 파라미터 형식으로 변환 (나중에 정규화를 위해)
                                        if schema and table:
                                            # FILA_ERP.DW_ETL_L -> #P_ERP_MS.$P_ERP_MS_OWN_FILA_ERP#.DW_ETL_L 형식으로 변환
                                            # 또는 그대로 사용
                                            table_name = f"{schema}.{table}" if schema else table
                                            # schema는 이미 설정됨
                        
                        # Vertica 테이블도 처리 (BIDW 관련)
                        elif "BIDW" in tabledef_value.upper() and not is_erp_in_tabledef:
                            parts = tabledef_value.split("\\")
                            if len(parts) >= 2:
                                last_part = parts[-1]
                                if "." in last_part:
                                    schema_table = last_part.split(".", 1)
                                    if len(schema_table) == 2:
                                        schema = schema_table[0]
                                        table = schema_table[1]
                                        table_name = table
                        
                        # 방법 3에서는 테이블만 찾고, Context 필터링은 마지막에 한 번만 적용
                
                # 방법 4: 정규식으로 직접 찾기 (XML 파싱 실패 시)
                if not table_name:
                    # XMLProperties 전체에서 TableName CDATA 찾기
                    xml_properties_match = re.search(r'XMLProperties.*?Value\s+(?:=+=+=+=)?(.*?)(?:=+=+=+=)?\s+END DSSUBRECORD', record_content, re.DOTALL)
                    if xml_properties_match:
                        xml_content = xml_properties_match.group(1)
                        # CDATA 안의 내용은 여러 줄일 수 있고 ]가 포함될 수 있으므로 더 정확한 패턴 사용
                        table_match = re.search(r'<TableName[^>]*><!\[CDATA\[(.*?)\]\]></TableName>', xml_content, re.DOTALL)
                        if table_match:
                            table_name = table_match.group(1).strip()
                
                        # 방법 4에서는 테이블만 찾고, Context 필터링은 마지막에 한 번만 적용
                        # Context가 없었던 경우 다시 확인
                        if table_name and not context_value:
                            context_match = re.search(r'<Context[^>]*>(\d+)</Context>', xml_content)
                            if context_match:
                                context_value = context_match.group(1).strip()
                
                # 방법 5: ODBCConnectorPX 타입의 Stage에서 Stage 이름으로 테이블 추론
                # Stage 이름이 S..., L_S..., T... 형식이면 뒤의 식별자가 실제 테이블명일 가능성이 높음
                if not table_name and is_connector and stage_name:
                    # Stage 이름 예시:
                    #   S_CM_USER_M, S2_OD_WM_INVN_MNTH, L_S4_FT_DD_SIZE_IN_RTN, T_FT_MM_SIZE_INVN
                    stage_name_upper = stage_name.upper()
                    
                    def _infer_from_stage(prefixes):
                        upper = stage_name_upper
                        for prefix in prefixes:
                            if upper.startswith(prefix) and len(stage_name) > len(prefix):
                                return stage_name[len(prefix):]
                        # S2_, S10_ 등 숫자 포함 패턴
                        match = re.match(r'(S\d+_)', upper)
                        if match and len(stage_name) > len(match.group(1)):
                            return stage_name[len(match.group(1)):]
                        match_l = re.match(r'(L_S\d+_)', upper)
                        if match_l and len(stage_name) > len(match_l.group(1)):
                            return stage_name[len(match_l.group(1)):]
                        match_t = re.match(r'(T\d+_)', upper)
                        if match_t and len(stage_name) > len(match_t.group(1)):
                            return stage_name[len(match_t.group(1)):]
                        return None
                    
                    inferred_table = None
                    if table_type == "source":
                        inferred_table = _infer_from_stage(["S_", "L_S_"])
                    elif table_type == "target":
                        inferred_table = _infer_from_stage(["T_", "L_T_"])
                    
                    if inferred_table:
                        # ERP 테이블일 가능성이 높으므로 스키마는 나중에 정규화 과정에서 보정
                        table_name = inferred_table
                        schema = ""
                
                # 테이블명에서 스키마와 테이블명 분리 및 Context 필터링 (단순화된 버전)
                if table_name:
                    # Context 필터링: 모든 방법에서 찾은 테이블에 대해 마지막에 한 번만 적용
                    if context_value:
                        # ERP 테이블인지 확인
                        is_erp_table = "#P_ERP" in table_name or "ERP" in table_name.upper()
                        
                        # Context 필터링: 1 = source, 2 = target
                        if table_type == "source" and context_value != "1":
                            # ERP 테이블인 경우 예외: Context가 2여도 소스로 간주
                            if not is_erp_table:
                                continue  # 필터링: 소스가 아닌 경우 제외
                        elif table_type == "target" and context_value != "2":
                            continue  # 필터링: 타겟이 아닌 경우 제외
                    else:
                        # Context가 없는 경우: Stage 이름으로 판단
                        stage_name_lower = (stage_name or "").lower()
                        
                        # Stage 이름에 "source", "input", "read", "from", "_s_", "s_" 등이 있으면 소스로 간주
                        is_source_stage = (
                            any(keyword in stage_name_lower for keyword in ["source", "input", "read", "from", "_s_", "_src"]) or
                            stage_name_lower.startswith("s_") or
                            stage_name_lower.startswith("l_s_") or
                            re.match(r's\d+_', stage_name_lower) is not None or
                            re.match(r'l_s\d+_', stage_name_lower) is not None
                        )
                        # Stage 이름에 "target", "output", "write", "to", "_t_", "_tgt" 등이 있으면 타겟으로 간주
                        is_target_stage = (
                            any(keyword in stage_name_lower for keyword in ["target", "output", "write", "to", "_t_", "_tgt"]) or
                            stage_name_lower.startswith("t_") or
                            stage_name_lower.startswith("l_t_") or
                            re.match(r't\d+_', stage_name_lower) is not None or
                            re.match(r'l_t\d+_', stage_name_lower) is not None
                        )
                        
                        # Stage 이름으로 판단 가능한 경우만 필터링
                        if is_source_stage and table_type != "source":
                            continue  # 필터링: 소스 Stage인데 타겟으로 요청한 경우 제외
                        elif is_target_stage and table_type != "target":
                            continue  # 필터링: 타겟 Stage인데 소스로 요청한 경우 제외
                        # Stage 이름으로도 판단 불가능한 경우는 추가하지 않음 (중복 방지)
                        elif not is_source_stage and not is_target_stage:
                            continue  # 필터링: 판단 불가능한 경우 제외
                    
                    # 파라미터 형식 처리: #P_DW_VER.$P_DW_VER_OWN_BIDWADM#.FT_AS_ACCP_RSLT
                    # 또는 일반 형식: SCHEMA.TABLE
                    original_table_name = table_name
                    if "." in table_name and not schema:
                        parts = table_name.rsplit(".", 1)
                        if len(parts) == 2:
                            potential_schema = parts[0]
                            potential_table = parts[1]
                            # 스키마가 파라미터가 아닌 실제 값인 경우
                            if not potential_schema.startswith("#") and potential_table:
                                schema = potential_schema
                                table_name = potential_table
                    
                    # 테이블명이 파라미터로 끝나는 경우 (예: #P_DW_VER.$P_DW_VER_OWN_BIDWADM_CO#.)
                    # 이런 경우는 실제 테이블명이 없으므로 스킵
                    if table_name.endswith("#.") or table_name == "#":
                        continue
                    
                    # 테이블명이 실제로 있는 경우만 추가
                    if table_name and table_name.strip():
                        tables.append({
                            "table_name": table_name,
                            "schema": schema or "",
                            "stage_name": stage_name,
                            "stage_type": olet_type or stage_type or "Unknown",
                            "table_type": table_type
                        })
                
        except Exception as e:
            logger.debug(f"테이블 추출 중 오류: {e}")
            import traceback
            logger.debug(traceback.format_exc())
        
        return tables
    
    def _extract_all_tables(self, content: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        모든 테이블 정보를 한 번에 추출 (source/target 구분)
        
        Args:
            content: DSX 파일 내용
            
        Returns:
            {"source_tables": [...], "target_tables": [...]} 딕셔너리
        """
        source_tables = []
        target_tables = []
        
        try:
            import xml.etree.ElementTree as ET
            
            # 모든 DSRECORD 찾기
            stage_pattern = r'BEGIN DSRECORD\s+Identifier\s+"([^"]+)"(.*?)END DSRECORD'
            total_records = 0
            tables_found = 0
            
            for match in re.finditer(stage_pattern, content, re.DOTALL):
                total_records += 1
                identifier = match.group(1)
                record_content = match.group(2)
                
                olet_type = self._extract_value(record_content, "OLEType")
                stage_name = self._extract_value(record_content, "Name") or identifier
                stage_type = self._extract_value(record_content, "StageType") or ""
                
                table_name = None
                schema = None
                table_type_determined = None  # "source" 또는 "target" 또는 None
                
                # 방법 0: TableDef에서 ERP 테이블 먼저 찾기 (가장 우선, XMLProperties 전)
                tabledef_match = re.search(r'TableDef\s+"([^"]+)"', record_content)
                if tabledef_match:
                    tabledef_value = tabledef_match.group(1)
                    is_erp_in_tabledef = "FILA_ERP" in tabledef_value or ("ERP" in tabledef_value.upper() and "FILA" in tabledef_value)
                    
                    if is_erp_in_tabledef:
                        # 경로에서 마지막 부분 추출
                        parts = tabledef_value.split("\\")
                        if len(parts) >= 2:
                            last_part = parts[-1]
                            if "." in last_part:
                                schema_table = last_part.split(".", 1)
                                if len(schema_table) == 2:
                                    temp_schema = schema_table[0]
                                    temp_table = schema_table[1]
                                    
                                    # dbo는 스키마가 아니므로 제거
                                    if temp_schema.lower() == "dbo":
                                        # FILA_ERP.dbo.DW_ETL_L -> FILA_ERP.DW_ETL_L
                                        for p in parts:
                                            if "FILA_ERP" in p:
                                                if "FILA_ERP_DW" in p:
                                                    temp_schema = "FILA_ERP_DW"
                                                else:
                                                    temp_schema = "FILA_ERP"
                                                break
                                    
                                    if temp_schema and temp_table:
                                        # 파라미터 형식으로 변환하여 저장
                                        table_name_param = f"#P_ERP_MS.$P_ERP_MS_OWN_{temp_schema}#.{temp_table}"
                                        # ERP 테이블은 보통 소스이므로 소스로 추가
                                        source_tables.append({
                                            "table_name": table_name_param,
                                            "schema": "",
                                            "stage_name": stage_name,
                                            "stage_type": olet_type or stage_type or "Unknown",
                                            "table_type": "source"
                                        })
                                        # 다음 레코드로 이동
                                        continue
                
                # 방법 1: 직접 TableName 필드 찾기
                table_name = self._extract_value(record_content, "TableName")
                schema = self._extract_value(record_content, "SchemaName")
                
                logger.debug(f"[테이블 추출] Record {total_records}: identifier={identifier}, stage_name={stage_name}, "
                           f"olet_type={olet_type}, method1_table={table_name}, method1_schema={schema}")
                
                # 방법 2: XMLProperties에서 TableName 추출 및 Context 확인
                xml_properties = self._extract_value(record_content, "XMLProperties")
                if xml_properties:
                    # =+=+=+= 로 감싸진 경우 처리
                    if xml_properties.startswith("=+=+=+="):
                        xml_properties = xml_properties[7:].strip()
                    if xml_properties.endswith("=+=+=+="):
                        xml_properties = xml_properties[:-7].strip()
                    
                    try:
                        root = ET.fromstring(xml_properties)
                        
                        # Context 확인 (source/target 구분)
                        context_value = None
                        # 방법 1: <Context>2</Context> 형식
                        context_elem = root.find(".//Context")
                        if context_elem is not None and context_elem.text:
                            context_value = context_elem.text.strip()
                        # 방법 2: <Context type='int'>2</Context> 형식 (속성으로도 확인)
                        if not context_value:
                            # 모든 Context 요소 확인
                            for ctx_elem in root.findall(".//Context"):
                                if ctx_elem.text:
                                    context_value = ctx_elem.text.strip()
                                    break
                        
                        # Context 값에 따라 타입 결정: 1 = source, 2 = target
                        if context_value:
                            if context_value == "1":
                                table_type_determined = "source"
                            elif context_value == "2":
                                table_type_determined = "target"
                        
                        # TableName 찾기 (방법 1에서 못 찾았을 때만)
                        if not table_name:
                            for table_elem in root.findall(".//TableName"):
                                if table_elem.text:
                                    table_name = table_elem.text.strip()
                                    break
                        
                        # SchemaName 찾기 (방법 1에서 못 찾았을 때만)
                        if not schema:
                            for schema_elem in root.findall(".//SchemaName"):
                                if schema_elem.text:
                                    schema = schema_elem.text.strip()
                                    break
                        
                        # SQL 문에서 테이블 추출 (TableName이 없는 경우)
                        if not table_name:
                            # SelectStatement 찾기
                            for sql_elem in root.findall(".//SelectStatement"):
                                if sql_elem.text:
                                    sql_text = sql_elem.text.strip()
                                    from_match = re.search(r'FROM\s+([^\s,;]+(?:\.[^\s,;]+)*)', sql_text, re.IGNORECASE | re.DOTALL)
                                    if from_match:
                                        table_ref = from_match.group(1).strip()
                                        table_ref = re.sub(r'\s+', '', table_ref)
                                        if "." in table_ref:
                                            parts = table_ref.rsplit(".", 1)
                                            if len(parts) == 2:
                                                if parts[0].startswith("#"):
                                                    table_name = table_ref
                                                else:
                                                    table_name = parts[1]
                                                    schema = parts[0]
                                        else:
                                            table_name = table_ref
                                        break
                            
                            # SQL 필드도 확인
                            if not table_name:
                                for sql_elem in root.findall(".//SQL"):
                                    if sql_elem.text:
                                        sql_text = sql_elem.text.strip()
                                        from_match = re.search(r'FROM\s+([^\s,;]+(?:\.[^\s,;]+)*)', sql_text, re.IGNORECASE | re.DOTALL)
                                        if from_match:
                                            table_ref = from_match.group(1).strip()
                                            table_ref = re.sub(r'\s+', '', table_ref)
                                            if "." in table_ref:
                                                parts = table_ref.rsplit(".", 1)
                                                if len(parts) == 2:
                                                    if parts[0].startswith("#"):
                                                        table_name = table_ref
                                                    else:
                                                        table_name = parts[1]
                                                        schema = parts[0]
                                            else:
                                                table_name = table_ref
                                            break
                                if table_name:
                                    break
                    
                    except (ET.ParseError, UnicodeDecodeError) as e:
                        logger.debug(f"XML 파싱 실패: {e}")
                        # XML 파싱 실패 시 정규식으로 시도
                        if not table_name:
                            table_match = re.search(r'<TableName[^>]*><!\[CDATA\[(.*?)\]\]></TableName>', xml_properties, re.DOTALL)
                            if table_match:
                                table_name = table_match.group(1).strip()
                            
                            if not schema:
                                schema_match = re.search(r'<SchemaName[^>]*><!\[CDATA\[(.*?)\]\]></SchemaName>', xml_properties, re.DOTALL)
                                if schema_match:
                                    schema = schema_match.group(1).strip()
                            
                            # Context도 정규식으로 찾기
                            if not table_type_determined:
                                context_match = re.search(r'<Context[^>]*><!\[CDATA\[(.*?)\]\]></Context>', xml_properties, re.DOTALL)
                                if context_match:
                                    context_value = context_match.group(1).strip()
                                    if context_value == "1":
                                        table_type_determined = "source"
                                    elif context_value == "2":
                                        table_type_determined = "target"
                            
                            # SQL 문에서 테이블 추출
                            if not table_name:
                                select_match = re.search(r'<SelectStatement[^>]*><!\[CDATA\[(.*?)\]\]></SelectStatement>', xml_properties, re.DOTALL)
                                if select_match:
                                    sql_text = select_match.group(1).strip()
                                    from_match = re.search(r'FROM\s+([^\s,;]+(?:\.[^\s,;]+)*)', sql_text, re.IGNORECASE | re.DOTALL)
                                    if from_match:
                                        table_ref = from_match.group(1).strip()
                                        table_ref = re.sub(r'\s+', '', table_ref)
                                        if "." in table_ref:
                                            parts = table_ref.rsplit(".", 1)
                                            if len(parts) == 2:
                                                if parts[0].startswith("#"):
                                                    table_name = table_ref
                                                else:
                                                    table_name = parts[1]
                                                    schema = parts[0]
                                        else:
                                            table_name = table_ref
                                
                                if not table_name:
                                    sql_match = re.search(r'<SQL[^>]*><!\[CDATA\[(.*?)\]\]></SQL>', xml_properties, re.DOTALL)
                                    if sql_match:
                                        sql_text = sql_match.group(1).strip()
                                        from_match = re.search(r'FROM\s+([^\s,;]+(?:\.[^\s,;]+)*)', sql_text, re.IGNORECASE | re.DOTALL)
                                        if from_match:
                                            table_ref = from_match.group(1).strip()
                                            table_ref = re.sub(r'\s+', '', table_ref)
                                            if "." in table_ref:
                                                parts = table_ref.rsplit(".", 1)
                                                if len(parts) == 2:
                                                    if parts[0].startswith("#"):
                                                        table_name = table_ref
                                                    else:
                                                        table_name = parts[1]
                                                        schema = parts[0]
                                            else:
                                                table_name = table_ref
                
                # 방법 3: 정규식으로 직접 찾기
                if not table_name:
                    xml_properties_match = re.search(r'XMLProperties.*?Value\s+(?:=+=+=+=)?(.*?)(?:=+=+=+=)?\s+END DSSUBRECORD', record_content, re.DOTALL)
                    if xml_properties_match:
                        xml_content = xml_properties_match.group(1)
                        table_match = re.search(r'<TableName[^>]*><!\[CDATA\[(.*?)\]\]></TableName>', xml_content, re.DOTALL)
                        if table_match:
                            table_name = table_match.group(1).strip()
                
                # 테이블명에서 스키마와 테이블명 분리
                if table_name:
                    original_table_name = table_name
                    if "." in table_name and not schema:
                        parts = table_name.rsplit(".", 1)
                        if len(parts) == 2:
                            potential_schema = parts[0]
                            potential_table = parts[1]
                            if not potential_schema.startswith("#") and potential_table:
                                schema = potential_schema
                                table_name = potential_table
                    
                    if table_name.endswith("#.") or table_name == "#":
                        continue
                    
                    if table_name and table_name.strip():
                        tables_found += 1
                        table_info = {
                            "table_name": table_name,
                            "schema": schema or "",
                            "stage_name": stage_name,
                            "stage_type": olet_type or stage_type or "Unknown",
                            "table_type": table_type_determined or "unknown"
                        }
                        
                        full_name = f"{schema}.{table_name}" if schema else table_name
                        logger.info(f"[테이블 추출 성공] {tables_found}번째: full_name={full_name}, "
                                  f"stage_name={stage_name}, context={table_type_determined}")
                        
                        # Context 값에 따라 source/target 분류
                        # Context가 명확하지 않으면 Stage 타입으로 판단 시도
                        if table_type_determined == "source":
                            source_tables.append(table_info)
                            logger.debug(f"  → source_tables에 추가")
                        elif table_type_determined == "target":
                            target_tables.append(table_info)
                            logger.debug(f"  → target_tables에 추가")
                        else:
                            # Context가 없을 때는 Stage 타입으로 판단
                            # 일반적으로 Database/ODBC Stage는 Context가 있어야 하지만,
                            # 없는 경우 Stage 이름이나 타입으로 추정
                            # 중복 방지를 위해 한쪽에만 추가 (일단 타겟으로 간주)
                            # (실제로는 Context가 있는 경우가 대부분이므로 이 경우는 드뭄)
                            stage_type_lower = (olet_type or stage_type or "").lower()
                            stage_name_lower = (stage_name or "").lower()
                            
                            # Stage 이름에 "source", "input", "read" 등이 있으면 소스로 간주
                            if any(keyword in stage_name_lower for keyword in ["source", "input", "read", "from"]):
                                source_tables.append(table_info)
                                logger.debug(f"  → source_tables에 추가 (Stage 이름으로 판단)")
                            # Stage 이름에 "target", "output", "write", "to" 등이 있으면 타겟으로 간주
                            elif any(keyword in stage_name_lower for keyword in ["target", "output", "write", "to", "_t_", "_tgt"]):
                                target_tables.append(table_info)
                                logger.debug(f"  → target_tables에 추가 (Stage 이름으로 판단)")
                            else:
                                # 판단 불가능한 경우는 추가하지 않음 (중복 방지)
                                logger.debug(f"  → Context 없음, Stage 이름으로도 판단 불가, 추가하지 않음")
                
        except Exception as e:
            logger.error(f"테이블 추출 중 오류: {e}")
            import traceback
            logger.debug(traceback.format_exc())
        
        logger.info(f"[테이블 추출 완료] 총 {total_records}개 레코드 스캔, {tables_found}개 테이블 발견, "
                   f"source={len(source_tables)}개, target={len(target_tables)}개")
        
        return {
            "source_tables": source_tables,
            "target_tables": target_tables
        }
    
    def scan_directory(self, directory: str, pattern: str = "*.dsx") -> List[Dict[str, Any]]:
        """
        디렉토리에서 DSX 파일 스캔 및 파싱
        
        Args:
            directory: 디렉토리 경로
            pattern: 파일 패턴 (기본값: *.dsx, "*"이면 모든 파일)
        
        Returns:
            Job 정보 리스트
        """
        jobs = []
        try:
            dir_path = Path(directory)
            if not dir_path.exists():
                logger.warning(f"디렉토리가 존재하지 않습니다: {directory}")
                return jobs
            
            # 패턴이 "*"이면 모든 파일 시도
            if pattern == "*":
                files_to_check = list(dir_path.iterdir())
            else:
                files_to_check = list(dir_path.glob(pattern))
            
            # 확장자 없는 파일도 포함 (DSX 형식인지 확인)
            if pattern == "*.dsx" or pattern == "*":
                # 확장자 없는 파일도 추가
                for file_path in dir_path.iterdir():
                    if file_path.is_file() and not file_path.suffix and file_path not in files_to_check:
                        files_to_check.append(file_path)
            
            for dsx_file in files_to_check:
                try:
                    # 파일이 DSX 형식인지 확인 (HEADER 섹션 확인)
                    if dsx_file.is_file():
                        try:
                            with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                                first_lines = ''.join(f.readlines()[:5])
                                if 'BEGIN HEADER' not in first_lines and 'BEGIN DSJOB' not in first_lines:
                                    continue  # DSX 형식이 아님
                        except:
                            continue
                    
                    # 여러 Job이 포함된 경우를 처리
                    with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    
                    # 여러 Job 파싱 시도
                    parsed_jobs = self.parse_multiple_jobs(content, str(dsx_file))
                    
                    if parsed_jobs:
                        # 여러 Job이 파싱된 경우
                        for job_info in parsed_jobs:
                            if job_info and job_info.get("name"):
                                jobs.append({
                                    "name": job_info["name"],
                                    "identifier": job_info.get("identifier"),
                                    "description": job_info.get("description"),
                                    "category": job_info.get("category"),
                                    "project": job_info.get("project"),
                                    "file_path": str(dsx_file),
                                    "source": "local_dsx"
                                })
                    else:
                        # 단일 Job으로 파싱 시도
                        job_info = self.parse_dsx_file(str(dsx_file))
                        if job_info and job_info.get("name"):
                            jobs.append({
                                "name": job_info["name"],
                                "identifier": job_info.get("identifier"),
                                "description": job_info.get("description"),
                                "category": job_info.get("category"),
                                "project": job_info.get("project"),
                                "file_path": str(dsx_file),
                                "source": "local_dsx"
                            })
                except Exception as e:
                    logger.debug(f"DSX 파일 파싱 실패: {dsx_file} - {e}")
                    continue
            
            logger.info(f"로컬 DSX 파일에서 {len(jobs)}개 Job 발견: {directory}")
        except Exception as e:
            logger.error(f"디렉토리 스캔 실패: {directory} - {e}")
        
        return jobs

