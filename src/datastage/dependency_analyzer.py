"""DataStage Job 의존성 분석 모듈"""

import re
from typing import Dict, Any, List, Optional, Set, Tuple
from pathlib import Path
from collections import defaultdict

from src.core.logger import get_logger
from src.datastage.dsx_parser import DSXParser
from src.datastage.parameter_mapper import ParameterMapper

logger = get_logger(__name__)


class DependencyAnalyzer:
    """DataStage Job 의존성 분석 클래스"""
    
    def __init__(self, export_directory: Optional[str] = None, resolve_parameters: bool = False):
        """
        의존성 분석기 초기화
        
        Args:
            export_directory: Export된 DSX 파일이 있는 디렉토리
            resolve_parameters: 파라미터를 실제 DB 정보로 해석할지 여부
        """
        self.dsx_parser = DSXParser()
        self.export_directory = Path(export_directory) if export_directory else None
        self._job_cache: Dict[str, Dict[str, Any]] = {}
        self.resolve_parameters = resolve_parameters
        self.parameter_mapper = ParameterMapper() if resolve_parameters else None
    
    def analyze_job_dependencies(self, dsx_file_path: str, job_content: Optional[str] = None) -> Dict[str, Any]:
        """
        Job의 테이블 의존성 분석
        
        Args:
            dsx_file_path: DSX 파일 경로
            job_content: 특정 Job의 내용 (여러 Job이 포함된 경우)
        
        Returns:
            의존성 정보 딕셔너리
        """
        try:
            if job_content:
                content = job_content
            else:
                with open(dsx_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
            
            job_info = self.dsx_parser.parse_dsx_content(content, dsx_file_path)
            if not job_info:
                return {}
            
            # 테이블 정보 추출
            source_tables = job_info.get("source_tables", [])
            target_tables = job_info.get("target_tables", [])
            
            # 모든 테이블 (스키마 포함)
            all_tables = []
            for table in source_tables + target_tables:
                schema = table.get("schema", "")
                table_name = table.get("table_name", "")
                if table_name:
                    full_name = f"{schema}.{table_name}" if schema else table_name
                    all_tables.append({
                        "full_name": full_name,
                        "schema": schema,
                        "table_name": table_name,
                        "type": table.get("table_type", "unknown"),
                        "stage_name": table.get("stage_name", ""),
                        "stage_type": table.get("stage_type", "")
                    })
            
            # 파라미터 해석 (옵션)
            if self.resolve_parameters and self.parameter_mapper:
                all_tables = self.parameter_mapper.map_tables(all_tables)
            
            # 컬럼 정보 추출
            columns = self._extract_columns(content)
            
            return {
                "job_name": job_info.get("name"),
                "file_path": dsx_file_path,
                "tables": all_tables,
                "columns": columns,
                "source_tables": source_tables,
                "target_tables": target_tables
            }
        except Exception as e:
            logger.error(f"의존성 분석 실패: {dsx_file_path} - {e}")
            return {}
    
    def _extract_columns(self, content: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        DSX 파일에서 컬럼 정보 추출
        
        Args:
            content: DSX 파일 내용
        
        Returns:
            테이블별 컬럼 정보 딕셔너리
        """
        columns_by_table = defaultdict(list)
        columns_seen = set()  # 중복 제거용
        
        try:
            logger.info(f"[컬럼 추출 시작]")
            # 방법 1: Stage의 TableName과 연결된 컬럼 찾기
            # Stage별로 컬럼 찾기
            stage_pattern = r'BEGIN DSRECORD\s+Identifier\s+"([^"]+)"(.*?)END DSRECORD'
            
            for stage_match in re.finditer(stage_pattern, content, re.DOTALL):
                stage_id = stage_match.group(1)
                stage_content = stage_match.group(2)
                
                # Stage 이름과 테이블 정보 추출
                stage_name = self._extract_value(stage_content, "Name") or stage_id
                table_name = self._extract_value(stage_content, "TableName")
                schema = self._extract_value(stage_content, "SchemaName")
                
                if table_name:
                    full_table_name = f"{schema}.{table_name}" if schema else table_name
                    logger.debug(f"[컬럼 추출] Stage {stage_id} ({stage_name}): table={full_table_name}")
                    
                    # 다양한 컬럼 패턴 시도
                    # 패턴 1: Column "COLUMN_NAME" Type "TYPE"
                    column_pattern1 = r'Column\s+"([^"]+)"\s+Type\s+"([^"]+)"'
                    col_count = 0
                    for col_match in re.finditer(column_pattern1, stage_content):
                        col_name = col_match.group(1)
                        col_type = col_match.group(2)
                        col_key = f"{full_table_name}::{col_name}"
                        if col_key not in columns_seen:
                            columns_by_table[full_table_name].append({
                                "name": col_name,
                                "type": col_type,
                                "stage_name": stage_name,
                                "stage_id": stage_id
                            })
                            columns_seen.add(col_key)
                            col_count += 1
                    if col_count > 0:
                        logger.debug(f"  → 패턴1로 {col_count}개 컬럼 발견")
                    
                    # 패턴 2: Column "COLUMN_NAME" (Type 없이)
                    column_pattern2 = r'Column\s+"([^"]+)"'
                    for col_match in re.finditer(column_pattern2, stage_content):
                        col_name = col_match.group(1)
                        col_key = f"{full_table_name}::{col_name}"
                        if col_key not in columns_seen:
                            columns_by_table[full_table_name].append({
                                "name": col_name,
                                "type": "Unknown",
                                "stage_name": stage_name,
                                "stage_id": stage_id
                            })
                            columns_seen.add(col_key)
            
            # 방법 2: Link에서 컬럼 찾기 (Link는 Stage 간 데이터 흐름을 나타냄)
            # Link는 보통 컬럼 정보를 포함함
            link_pattern = r'BEGIN DSRECORD\s+Identifier\s+"([^"]+)"(.*?)END DSRECORD'
            
            for link_match in re.finditer(link_pattern, content, re.DOTALL):
                link_id = link_match.group(1)
                link_content = link_match.group(2)
                
                # Link 타입 확인 (CTrxOutput, CCustomInput, CTrxInput 등도 Link)
                olet_type = self._extract_value(link_content, "OLEType")
                link_name = self._extract_value(link_content, "Name")
                # Link는 OLEType에 "Link"가 있거나, "Output", "Input"이 포함된 경우
                is_link = (olet_type and ("Link" in olet_type or "Output" in olet_type or "Input" in olet_type)) or link_name
                if is_link:
                    # Link의 SourceStage와 TargetStage 확인
                    source_stage = self._extract_value(link_content, "SourceStage")
                    target_stage = self._extract_value(link_content, "TargetStage")
                    
                    # Partner 필드에서 Stage 정보 추출 (Partner "V2S3|V2S3P1" 형식)
                    if not source_stage or not target_stage:
                        partner = self._extract_value(link_content, "Partner")
                        if partner:
                            # Partner는 "SourceStage|SourcePin|TargetStage|TargetPin" 형식
                            parts = partner.split("|")
                            if len(parts) >= 2:
                                if not source_stage:
                                    source_stage = parts[0]
                                if len(parts) >= 3 and not target_stage:
                                    target_stage = parts[2]
                    
                    # Link의 컬럼 찾기
                    # 패턴 1: Column "COLUMN_NAME" Type "TYPE"
                    column_pattern1 = r'Column\s+"([^"]+)"\s+Type\s+"([^"]+)"'
                    for col_match in re.finditer(column_pattern1, link_content):
                        col_name = col_match.group(1)
                        col_type = col_match.group(2)
                        
                        # Source Stage의 테이블 정보 찾기
                        source_table = self._find_table_for_stage(content, source_stage)
                        if source_table:
                            full_table_name = source_table["full_name"]
                            col_key = f"{full_table_name}::{col_name}"
                            if col_key not in columns_seen:
                                columns_by_table[full_table_name].append({
                                    "name": col_name,
                                    "type": col_type,
                                    "stage_name": source_table.get("stage_name", source_stage),
                                    "stage_id": source_stage,
                                    "link_id": link_id
                                })
                                columns_seen.add(col_key)
                        
                        # Target Stage의 테이블 정보도 찾기 (Target Stage가 테이블을 사용하는 경우)
                        target_table = self._find_table_for_stage(content, target_stage)
                        if target_table:
                            full_table_name = target_table["full_name"]
                            col_key = f"{full_table_name}::{col_name}"
                            if col_key not in columns_seen:
                                columns_by_table[full_table_name].append({
                                    "name": col_name,
                                    "type": col_type,
                                    "stage_name": target_table.get("stage_name", target_stage),
                                    "stage_id": target_stage,
                                    "link_id": link_id
                                })
                                columns_seen.add(col_key)
                    
                    # 패턴 2: Column "COLUMN_NAME" (Type 없이)
                    column_pattern2 = r'Column\s+"([^"]+)"'
                    for col_match in re.finditer(column_pattern2, link_content):
                        col_name = col_match.group(1)
                        
                        # Source Stage의 테이블 정보 찾기
                        source_table = self._find_table_for_stage(content, source_stage)
                        if source_table:
                            full_table_name = source_table["full_name"]
                            col_key = f"{full_table_name}::{col_name}"
                            if col_key not in columns_seen:
                                columns_by_table[full_table_name].append({
                                    "name": col_name,
                                    "type": "Unknown",
                                    "stage_name": source_table.get("stage_name", source_stage),
                                    "stage_id": source_stage,
                                    "link_id": link_id
                                })
                                columns_seen.add(col_key)
                        
                        # Target Stage의 테이블 정보도 찾기
                        target_table = self._find_table_for_stage(content, target_stage)
                        if target_table:
                            full_table_name = target_table["full_name"]
                            col_key = f"{full_table_name}::{col_name}"
                            if col_key not in columns_seen:
                                columns_by_table[full_table_name].append({
                                    "name": col_name,
                                    "type": "Unknown",
                                    "stage_name": target_table.get("stage_name", target_stage),
                                    "stage_id": target_stage,
                                    "link_id": link_id
                                })
                                columns_seen.add(col_key)
                    
                    # 패턴 3: Columns "COutputColumn" 다음의 DSSUBRECORD에서 컬럼 추출
                    # Columns "COutputColumn" BEGIN DSSUBRECORD Name "COLUMN_NAME" ...
                    if 'Columns "COutputColumn"' in link_content or 'Columns "CInputColumn"' in link_content:
                        # 각 컬럼은 BEGIN DSSUBRECORD ... Name "COLUMN_NAME" ... END DSSUBRECORD 형식
                        column_subrecord_pattern = r'BEGIN DSSUBRECORD\s+Name\s+"([^"]+)"(.*?)END DSSUBRECORD'
                        for col_match in re.finditer(column_subrecord_pattern, link_content, re.DOTALL):
                            col_name = col_match.group(1)
                            col_subrecord = col_match.group(2)
                            
                            # 컬럼 타입 찾기
                            col_type = self._extract_value(col_subrecord, "SqlType") or "Unknown"
                            col_precision = self._extract_value(col_subrecord, "Precision")
                            col_scale = self._extract_value(col_subrecord, "Scale")
                            is_nullable = self._extract_value(col_subrecord, "Nullable") == "1"
                            
                            type_str = col_type
                            if col_precision:
                                type_str += f"({col_precision}"
                                if col_scale:
                                    type_str += f",{col_scale}"
                                type_str += ")"
                            
                            # Source Stage의 테이블 정보 찾기
                            source_table = self._find_table_for_stage(content, source_stage)
                            if source_table:
                                full_table_name = source_table["full_name"]
                                col_key = f"{full_table_name}::{col_name}"
                                if col_key not in columns_seen:
                                    columns_by_table[full_table_name].append({
                                        "name": col_name,
                                        "type": type_str,
                                        "stage_name": source_table.get("stage_name", source_stage),
                                        "stage_id": source_stage,
                                        "link_id": link_id,
                                        "nullable": is_nullable
                                    })
                                    columns_seen.add(col_key)
                            
                            # Target Stage의 테이블 정보도 찾기
                            target_table = self._find_table_for_stage(content, target_stage)
                            if target_table:
                                full_table_name = target_table["full_name"]
                                col_key = f"{full_table_name}::{col_name}"
                                if col_key not in columns_seen:
                                    columns_by_table[full_table_name].append({
                                        "name": col_name,
                                        "type": type_str,
                                        "stage_name": target_table.get("stage_name", target_stage),
                                        "stage_id": target_stage,
                                        "link_id": link_id,
                                        "nullable": is_nullable
                                    })
                                    columns_seen.add(col_key)
                    
                    # 패턴 4: Schema의 record(...) 형식에서 컬럼 추출
                    # Schema Value =+=+=+= record ( COLUMN_NAME:type; ... ) =+=+=+=
                    schema_match = re.search(r'Name\s+"Schema"\s+Value\s+(?:=+=+=+=)?\s*record\s*\((.*?)\)\s*(?:=+=+=+=)?', link_content, re.DOTALL)
                    if schema_match:
                        schema_content = schema_match.group(1)
                        # 컬럼 패턴: COLUMN_NAME:nullable? type;
                        # 예: CUST_NO:ustring[max=60]; 또는 CUST_CRD_NO:nullable ustring[max=30];
                        column_record_pattern = r'(\w+)\s*:\s*(nullable\s+)?([^;]+);'
                        for col_match in re.finditer(column_record_pattern, schema_content):
                            col_name = col_match.group(1)
                            is_nullable = col_match.group(2) is not None
                            col_type = col_match.group(3).strip()
                            
                            # Source Stage의 테이블 정보 찾기
                            source_table = self._find_table_for_stage(content, source_stage)
                            if source_table:
                                full_table_name = source_table["full_name"]
                                col_key = f"{full_table_name}::{col_name}"
                                if col_key not in columns_seen:
                                    columns_by_table[full_table_name].append({
                                        "name": col_name,
                                        "type": col_type,
                                        "stage_name": source_table.get("stage_name", source_stage),
                                        "stage_id": source_stage,
                                        "link_id": link_id,
                                        "nullable": is_nullable
                                    })
                                    columns_seen.add(col_key)
                            
                            # Target Stage의 테이블 정보도 찾기
                            target_table = self._find_table_for_stage(content, target_stage)
                            if target_table:
                                full_table_name = target_table["full_name"]
                                col_key = f"{full_table_name}::{col_name}"
                                if col_key not in columns_seen:
                                    columns_by_table[full_table_name].append({
                                        "name": col_name,
                                        "type": col_type,
                                        "stage_name": target_table.get("stage_name", target_stage),
                                        "stage_id": target_stage,
                                        "link_id": link_id,
                                        "nullable": is_nullable
                                    })
                                    columns_seen.add(col_key)
            
            # 방법 3: 전체 파일에서 컬럼명 직접 검색 (테이블 정보와 연결)
            # 컬럼명이 나타나는 모든 위치 찾기
            # 일반적인 컬럼명 패턴: "COLUMN_NAME" (따옴표로 둘러싸인 대문자/숫자/언더스코어)
            column_name_pattern = r'"([A-Z][A-Z0-9_]*)"'
            
            # 모든 Stage의 테이블 정보 먼저 수집
            all_tables = {}
            for stage_match in re.finditer(stage_pattern, content, re.DOTALL):
                stage_id = stage_match.group(1)
                stage_content = stage_match.group(2)
                table_name = self._extract_value(stage_content, "TableName")
                schema = self._extract_value(stage_content, "SchemaName")
                stage_name = self._extract_value(stage_content, "Name") or stage_id
                
                if table_name:
                    full_table_name = f"{schema}.{table_name}" if schema else table_name
                    all_tables[stage_id] = {
                        "full_name": full_table_name,
                        "table_name": table_name,
                        "schema": schema,
                        "stage_name": stage_name
                    }
            
            # 각 Stage 주변에서 컬럼명 찾기
            for stage_id, table_info in all_tables.items():
                # Stage 주변 영역 찾기 (Stage DSRECORD와 연결된 Link들)
                stage_area_pattern = rf'BEGIN DSRECORD\s+Identifier\s+"{re.escape(stage_id)}"(.*?)END DSRECORD'
                stage_match = re.search(stage_area_pattern, content, re.DOTALL)
                if stage_match:
                    stage_area = stage_match.group(1)
                    
                    # 이 Stage와 연결된 Link 찾기
                    link_pattern = rf'BEGIN DSRECORD\s+Identifier\s+"([^"]+)"(.*?)END DSRECORD'
                    for link_match in re.finditer(link_pattern, content, re.DOTALL):
                        link_content = link_match.group(2)
                        source_stage = self._extract_value(link_content, "SourceStage")
                        target_stage = self._extract_value(link_content, "TargetStage")
                        
                        if source_stage == stage_id or target_stage == stage_id:
                            # Link에서 컬럼명 찾기
                            for col_match in re.finditer(column_name_pattern, link_content):
                                potential_col = col_match.group(1)
                                # 컬럼명으로 보이는 패턴 (대문자, 숫자, 언더스코어, 길이 2 이상)
                                if len(potential_col) >= 2 and potential_col.replace('_', '').replace('0', '').replace('1', '').replace('2', '').replace('3', '').replace('4', '').replace('5', '').replace('6', '').replace('7', '').replace('8', '').replace('9', '').isalpha():
                                    full_table_name = table_info["full_name"]
                                    col_key = f"{full_table_name}::{potential_col}"
                                    if col_key not in columns_seen:
                                        columns_by_table[full_table_name].append({
                                            "name": potential_col,
                                            "type": "Unknown",
                                            "stage_name": table_info["stage_name"],
                                            "stage_id": stage_id
                                        })
                                        columns_seen.add(col_key)
        
        except Exception as e:
            logger.error(f"컬럼 추출 중 오류: {e}")
            import traceback
            logger.debug(traceback.format_exc())
        
        result = dict(columns_by_table)
        total_columns = sum(len(cols) for cols in result.values())
        logger.info(f"[컬럼 추출 완료] {len(result)}개 테이블에서 총 {total_columns}개 컬럼 발견")
        for table_name, cols in result.items():
            logger.debug(f"  - {table_name}: {len(cols)}개 컬럼")
        
        return result
    
    def _find_table_for_stage(self, content: str, stage_id: str) -> Optional[Dict[str, Any]]:
        """Stage ID로 테이블 정보 찾기"""
        try:
            stage_pattern = rf'BEGIN DSRECORD\s+Identifier\s+"{re.escape(stage_id)}"(.*?)END DSRECORD'
            stage_match = re.search(stage_pattern, content, re.DOTALL)
            if stage_match:
                stage_content = stage_match.group(1)
                table_name = self._extract_value(stage_content, "TableName")
                schema = self._extract_value(stage_content, "SchemaName")
                stage_name = self._extract_value(stage_content, "Name") or stage_id
                
                if table_name:
                    return {
                        "full_name": f"{schema}.{table_name}" if schema else table_name,
                        "table_name": table_name,
                        "schema": schema,
                        "stage_name": stage_name
                    }
        except Exception as e:
            logger.debug(f"Stage 테이블 찾기 실패: {stage_id} - {e}")
        return None
    
    def _extract_value(self, content: str, key: str) -> Optional[str]:
        """레코드 내용에서 키 값 추출"""
        pattern = rf'{key}\s+"([^"]+)"'
        match = re.search(pattern, content)
        if match:
            return match.group(1)
        return None
    
    def find_jobs_using_table(self, table_name: str, schema: Optional[str] = None,
                              export_directory: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        특정 테이블을 사용하는 Job 찾기
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름 (선택)
            export_directory: Export 디렉토리 (None이면 초기화 시 설정한 값 사용)
        
        Returns:
            해당 테이블을 사용하는 Job 리스트
        """
        directory = Path(export_directory) if export_directory else self.export_directory
        if not directory or not directory.exists():
            logger.warning(f"Export 디렉토리가 존재하지 않습니다: {directory}")
            return []
        
        matching_jobs = []
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        # DSX 파일 스캔
        dsx_files = list(directory.glob("*.dsx"))
        dsx_files.extend([f for f in directory.iterdir() if f.is_file() and not f.suffix])
        
        for dsx_file in dsx_files:
            try:
                # 파일이 DSX 형식인지 확인
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    first_lines = ''.join(f.readlines()[:5])
                    if 'BEGIN HEADER' not in first_lines and 'BEGIN DSJOB' not in first_lines:
                        continue
                
                # 여러 Job이 포함된 경우 처리
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                from src.datastage.dsx_parser import DSXParser
                parser = DSXParser()
                parsed_jobs = parser.parse_multiple_jobs(content, str(dsx_file))
                
                if parsed_jobs and len(parsed_jobs) > 1:
                    # 여러 Job이 포함된 경우 - 각 Job별로 확인
                    import re
                    dsjob_pattern = r'BEGIN DSJOB\s+(.*?)\s+END DSJOB'
                    dsjob_matches = list(re.finditer(dsjob_pattern, content, re.DOTALL))
                    
                    for i, job_info in enumerate(parsed_jobs):
                        job_name = job_info.get("name")
                        if not job_name:
                            continue
                        
                        # 해당 Job의 내용 추출하여 분석
                        if i < len(dsjob_matches):
                            dsjob_start = dsjob_matches[i].start()
                            if i + 1 < len(dsjob_matches):
                                next_dsjob_start = dsjob_matches[i + 1].start()
                                job_content = content[dsjob_start:next_dsjob_start]
                            else:
                                job_content = content[dsjob_start:]
                            
                            deps = self.analyze_job_dependencies(str(dsx_file), job_content)
                        else:
                            # Job 정보 직접 사용
                            deps = {
                                "job_name": job_name,
                                "file_path": str(dsx_file),
                                "tables": []
                            }
                            for table in job_info.get("source_tables", []) + job_info.get("target_tables", []):
                                schema_val = table.get("schema", "")
                                table_name_val = table.get("table_name", "")
                                if table_name_val:
                                    full_name = f"{schema_val}.{table_name_val}" if schema_val else table_name_val
                                    deps["tables"].append({
                                        "full_name": full_name,
                                        "schema": schema_val,
                                        "table_name": table_name_val
                                    })
                        
                        # 테이블 사용 여부 확인
                        for table in deps.get("tables", []):
                            if (table.get("table_name", "").upper() == table_name.upper() and
                                (not schema or table.get("schema", "").upper() == schema.upper())):
                                matching_jobs.append({
                                    "job_name": deps.get("job_name"),
                                    "file_path": str(dsx_file),
                                    "table_usage": table,
                                    "all_tables": deps.get("tables", [])
                                })
                                break
                else:
                    # 단일 Job으로 분석
                    deps = self.analyze_job_dependencies(str(dsx_file))
                    
                    # 테이블 사용 여부 확인
                    for table in deps.get("tables", []):
                        if (table.get("table_name", "").upper() == table_name.upper() and
                            (not schema or table.get("schema", "").upper() == schema.upper())):
                            matching_jobs.append({
                                "job_name": deps.get("job_name"),
                                "file_path": str(dsx_file),
                                "table_usage": table,
                                "all_tables": deps.get("tables", [])
                            })
                            break
            except Exception as e:
                logger.debug(f"Job 분석 실패: {dsx_file} - {e}")
                continue
        
        logger.info(f"테이블 '{full_table_name}'을 사용하는 Job {len(matching_jobs)}개 발견")
        return matching_jobs
    
    def find_jobs_using_column(self, table_name: str, column_name: str,
                               schema: Optional[str] = None,
                               export_directory: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        특정 컬럼을 사용하는 Job 찾기
        
        Args:
            table_name: 테이블 이름
            column_name: 컬럼 이름
            schema: 스키마 이름 (선택)
            export_directory: Export 디렉토리
        
        Returns:
            해당 컬럼을 사용하는 Job 리스트
        """
        directory = Path(export_directory) if export_directory else self.export_directory
        if not directory or not directory.exists():
            logger.warning(f"Export 디렉토리가 존재하지 않습니다: {directory}")
            return []
        
        matching_jobs = []
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        # DSX 파일 스캔
        dsx_files = list(directory.glob("*.dsx"))
        dsx_files.extend([f for f in directory.iterdir() if f.is_file() and not f.suffix])
        
        for dsx_file in dsx_files:
            try:
                # 파일이 DSX 형식인지 확인
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    first_lines = ''.join(f.readlines()[:5])
                    if 'BEGIN HEADER' not in first_lines and 'BEGIN DSJOB' not in first_lines:
                        continue
                
                # 여러 Job이 포함된 경우 처리
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                from src.datastage.dsx_parser import DSXParser
                parser = DSXParser()
                parsed_jobs = parser.parse_multiple_jobs(content, str(dsx_file))
                
                if parsed_jobs and len(parsed_jobs) > 1:
                    # 여러 Job이 포함된 경우 - 각 Job별로 확인
                    import re
                    dsjob_pattern = r'BEGIN DSJOB\s+(.*?)\s+END DSJOB'
                    dsjob_matches = list(re.finditer(dsjob_pattern, content, re.DOTALL))
                    
                    for i, job_info in enumerate(parsed_jobs):
                        job_name = job_info.get("name")
                        if not job_name:
                            continue
                        
                        # 해당 Job의 내용 추출하여 분석
                        if i < len(dsjob_matches):
                            dsjob_start = dsjob_matches[i].start()
                            if i + 1 < len(dsjob_matches):
                                next_dsjob_start = dsjob_matches[i + 1].start()
                                job_content = content[dsjob_start:next_dsjob_start]
                            else:
                                job_content = content[dsjob_start:]
                            
                            deps = self.analyze_job_dependencies(str(dsx_file), job_content)
                        else:
                            deps = {"job_name": job_name, "file_path": str(dsx_file), "columns": {}}
                        
                        # 컬럼 사용 여부 확인
                        columns = deps.get("columns", {})
                        if full_table_name in columns:
                            for col in columns[full_table_name]:
                                if col.get("name", "").upper() == column_name.upper():
                                    matching_jobs.append({
                                        "job_name": deps.get("job_name"),
                                        "file_path": str(dsx_file),
                                        "column_usage": col,
                                        "table_name": full_table_name,
                                        "all_columns": columns.get(full_table_name, [])
                                    })
                                    break
                else:
                    # 단일 Job으로 분석
                    deps = self.analyze_job_dependencies(str(dsx_file))
                    
                    # 컬럼 사용 여부 확인
                    columns = deps.get("columns", {})
                    if full_table_name in columns:
                        for col in columns[full_table_name]:
                            if col.get("name", "").upper() == column_name.upper():
                                matching_jobs.append({
                                    "job_name": deps.get("job_name"),
                                    "file_path": str(dsx_file),
                                    "column_usage": col,
                                    "table_name": full_table_name,
                                    "all_columns": columns.get(full_table_name, [])
                                })
                                break
            except Exception as e:
                logger.debug(f"Job 분석 실패: {dsx_file} - {e}")
                continue
        
        logger.info(f"컬럼 '{full_table_name}.{column_name}'을 사용하는 Job {len(matching_jobs)}개 발견")
        return matching_jobs
    
    def find_tables_using_column(self, column_name: str,
                                 export_directory: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        특정 컬럼명을 포함하는 모든 테이블 찾기
        컬럼명을 사용하는 Job을 먼저 찾고, 그 Job에서 사용하는 테이블들을 수집
        
        Args:
            column_name: 컬럼 이름
            export_directory: Export 디렉토리
        
        Returns:
            해당 컬럼을 포함하는 테이블 리스트 (테이블 정보와 사용하는 Job 정보 포함)
        """
        directory = Path(export_directory) if export_directory else self.export_directory
        if not directory or not directory.exists():
            logger.warning(f"Export 디렉토리가 존재하지 않습니다: {directory}")
            return []
        
        # 컬럼명을 사용하는 Job 찾기
        jobs_with_column = self.find_jobs_using_column_only(column_name, export_directory=str(directory))
        
        # Job에서 사용하는 테이블 수집
        tables_dict = {}  # full_name -> {table_info, jobs}
        
        for job in jobs_with_column:
            job_name = job.get("job_name")
            file_path = job.get("file_path")
            all_tables = job.get("all_tables", [])
            
            # Job의 모든 테이블에 대해
            for table_full_name in all_tables:
                if not table_full_name or table_full_name == "Unknown":
                    continue
                
                if table_full_name not in tables_dict:
                    # 테이블 정보 파싱
                    schema = None
                    table_name = table_full_name
                    if "." in table_full_name:
                        parts = table_full_name.split(".", 1)
                        schema = parts[0]
                        table_name = parts[1]
                    
                    tables_dict[table_full_name] = {
                        "table_name": table_name,
                        "schema": schema,
                        "full_name": table_full_name,
                        "column_name": column_name,
                        "related_jobs": [],
                        "job_count": 0
                    }
                
                # Job 추가 (중복 제거)
                job_info = {"job_name": job_name, "file_path": file_path}
                if job_info not in tables_dict[table_full_name]["related_jobs"]:
                    tables_dict[table_full_name]["related_jobs"].append(job_info)
                    tables_dict[table_full_name]["job_count"] += 1
        
        tables_with_column = list(tables_dict.values())
        
        logger.info(f"컬럼 '{column_name}'을 포함하는 테이블 {len(tables_with_column)}개 발견")
        return tables_with_column
    
    def find_jobs_using_column_only(self, column_name: str,
                                    export_directory: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        컬럼명만으로 해당 컬럼을 사용하는 모든 Job 찾기 (모든 테이블 포함)
        컬럼명이 파일에 나타나는 모든 Job을 찾고, 그 Job에서 사용하는 테이블들을 역으로 찾음
        
        Args:
            column_name: 컬럼 이름
            export_directory: Export 디렉토리
        
        Returns:
            해당 컬럼을 사용하는 Job 리스트 (테이블 정보 포함)
        """
        directory = Path(export_directory) if export_directory else self.export_directory
        if not directory or not directory.exists():
            logger.warning(f"Export 디렉토리가 존재하지 않습니다: {directory}")
            return []
        
        matching_jobs = []
        job_seen = set()  # 중복 제거용
        
        # DSX 파일 스캔
        dsx_files = list(directory.glob("*.dsx"))
        dsx_files.extend([f for f in directory.iterdir() if f.is_file() and not f.suffix])
        
        for dsx_file in dsx_files:
            try:
                # 파일이 DSX 형식인지 확인
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    first_lines = ''.join(f.readlines()[:5])
                    if 'BEGIN HEADER' not in first_lines and 'BEGIN DSJOB' not in first_lines:
                        continue
                
                # 여러 Job이 포함된 경우 처리
                # 큰 파일도 처리할 수 있도록 청크 단위로 읽기
                file_size = dsx_file.stat().st_size
                content = ""
                
                # 파일이 너무 크면 (100MB 이상) 청크로 읽기
                if file_size > 100 * 1024 * 1024:
                    # 먼저 컬럼명이 있는지 빠르게 확인
                    with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                        # 처음 10MB와 끝 10MB만 확인
                        head = f.read(10 * 1024 * 1024)
                        if file_size > 20 * 1024 * 1024:
                            f.seek(max(0, file_size - 10 * 1024 * 1024))
                            tail = f.read(10 * 1024 * 1024)
                            sample = head + tail
                        else:
                            sample = head
                    
                    # 샘플에서 컬럼명 확인
                    column_name_upper = column_name.upper()
                    column_variants = [
                        column_name_upper,
                        column_name_upper.replace('_', ''),
                        column_name_upper.replace('_', ' '),
                    ]
                    
                    found_in_sample = False
                    sample_upper = sample.upper()
                    for variant in column_variants:
                        if variant in sample_upper:
                            found_in_sample = True
                            break
                    
                    if not found_in_sample:
                        # 샘플에 없어도 전체 파일을 읽어서 확인
                        # 하지만 너무 크면 스킵
                        if file_size > 500 * 1024 * 1024:  # 500MB 이상이면 스킵
                            logger.debug(f"파일이 너무 큼, 스킵: {dsx_file.name} ({file_size / 1024 / 1024:.1f}MB)")
                            continue
                        # 전체 파일 읽기
                        with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                    else:
                        # 샘플에 있으면 전체 파일 읽기
                        with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                else:
                    # 작은 파일은 전체 읽기
                    with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                
                # 컬럼명이 파일에 있는지 확인 (빠른 필터링)
                # 대소문자 무관하게 검색
                content_upper = content.upper()
                column_name_upper = column_name.upper()
                
                # 컬럼명이 직접 나타나거나, 언더스코어가 없는 형태로 나타날 수 있음
                # 예: STYL_CD -> STYLCD
                column_variants = [
                    column_name_upper,
                    column_name_upper.replace('_', ''),
                    column_name_upper.replace('_', ' '),
                ]
                
                found_variant = False
                for variant in column_variants:
                    if variant in content_upper:
                        found_variant = True
                        break
                
                if not found_variant:
                    continue
                
                from src.datastage.dsx_parser import DSXParser
                parser = DSXParser()
                parsed_jobs = parser.parse_multiple_jobs(content, str(dsx_file))
                
                if parsed_jobs and len(parsed_jobs) > 1:
                    # 여러 Job이 포함된 경우 - 각 Job별로 확인
                    import re
                    dsjob_pattern = r'BEGIN DSJOB\s+(.*?)\s+END DSJOB'
                    dsjob_matches = list(re.finditer(dsjob_pattern, content, re.DOTALL))
                    
                    for i, job_info in enumerate(parsed_jobs):
                        job_name = job_info.get("name")
                        if not job_name:
                            continue
                        
                        # 해당 Job의 내용 추출
                        if i < len(dsjob_matches):
                            dsjob_start = dsjob_matches[i].start()
                            if i + 1 < len(dsjob_matches):
                                next_dsjob_start = dsjob_matches[i + 1].start()
                                job_content = content[dsjob_start:next_dsjob_start]
                            else:
                                job_content = content[dsjob_start:]
                        else:
                            job_content = content
                        
                        # 컬럼명이 이 Job에 있는지 확인 (다양한 변형 포함)
                        job_content_upper = job_content.upper()
                        found_in_job = False
                        for variant in column_variants:
                            if variant in job_content_upper:
                                found_in_job = True
                                break
                        
                        if not found_in_job:
                            continue
                        
                        # Job의 의존성 분석
                        deps = self.analyze_job_dependencies(str(dsx_file), job_content)
                        
                        # 모든 테이블에서 컬럼 찾기
                        columns = deps.get("columns", {})
                        found_in_tables = []
                        for table_full_name, cols in columns.items():
                            for col in cols:
                                if col.get("name", "").upper() == column_name.upper():
                                    found_in_tables.append(table_full_name)
                                    break
                        
                        # 컬럼이 columns 딕셔너리에 없어도, Job에 컬럼명이 나타나면 포함
                        # (컬럼 추출이 실패했을 수 있으므로)
                        if not found_in_tables:
                            # Job의 모든 테이블을 가져옴
                            all_tables = deps.get("tables", [])
                            for table in all_tables:
                                found_in_tables.append(table.get("full_name", ""))
                        
                        if found_in_tables:
                            job_key = f"{job_name}::{str(dsx_file)}"
                            if job_key not in job_seen:
                                matching_jobs.append({
                                    "job_name": job_name,
                                    "file_path": str(dsx_file),
                                    "table_name": found_in_tables[0] if found_in_tables else "Unknown",
                                    "all_tables": found_in_tables,
                                    "column_name": column_name
                                })
                                job_seen.add(job_key)
                else:
                    # 단일 Job으로 분석
                    # 컬럼명이 파일에 있는지 확인 (이미 위에서 확인함)
                    # content는 이미 읽혀있고, found_variant도 확인됨
                    
                    deps = self.analyze_job_dependencies(str(dsx_file))
                    job_name = deps.get("job_name", "Unknown")
                    
                    # 모든 테이블에서 컬럼 찾기
                    columns = deps.get("columns", {})
                    found_in_tables = []
                    for table_full_name, cols in columns.items():
                        for col in cols:
                            if col.get("name", "").upper() == column_name.upper():
                                found_in_tables.append(table_full_name)
                                break
                    
                    # 컬럼이 columns 딕셔너리에 없어도, Job에 컬럼명이 나타나면 포함
                    if not found_in_tables:
                        all_tables = deps.get("tables", [])
                        for table in all_tables:
                            found_in_tables.append(table.get("full_name", ""))
                    
                    if found_in_tables:
                        job_key = f"{job_name}::{str(dsx_file)}"
                        if job_key not in job_seen:
                            matching_jobs.append({
                                "job_name": job_name,
                                "file_path": str(dsx_file),
                                "table_name": found_in_tables[0] if found_in_tables else "Unknown",
                                "all_tables": found_in_tables,
                                "column_name": column_name
                            })
                            job_seen.add(job_key)
            except Exception as e:
                logger.debug(f"Job 분석 실패: {dsx_file} - {e}")
                continue
        
        logger.info(f"컬럼 '{column_name}'을 사용하는 Job {len(matching_jobs)}개 발견 (모든 테이블 포함)")
        return matching_jobs
    
    def analyze_all_dependencies(self, export_directory: Optional[str] = None) -> Dict[str, Any]:
        """
        Export 디렉토리의 모든 Job 의존성 분석
        
        Args:
            export_directory: Export 디렉토리
        
        Returns:
            전체 의존성 정보 딕셔너리
        """
        directory = Path(export_directory) if export_directory else self.export_directory
        if not directory or not directory.exists():
            logger.warning(f"Export 디렉토리가 존재하지 않습니다: {directory}")
            return {}
        
        all_dependencies = {
            "jobs": [],
            "tables": defaultdict(list),
            "columns": defaultdict(lambda: defaultdict(list))
        }
        
        # DSX 파일 스캔
        dsx_files = list(directory.glob("*.dsx"))
        dsx_files.extend([f for f in directory.iterdir() if f.is_file() and not f.suffix])
        
        for dsx_file in dsx_files:
            try:
                # 파일이 DSX 형식인지 확인
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    first_lines = ''.join(f.readlines()[:5])
                    if 'BEGIN HEADER' not in first_lines and 'BEGIN DSJOB' not in first_lines:
                        continue
                
                # 여러 Job이 포함된 경우 처리
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                # 여러 Job 파싱
                from src.datastage.dsx_parser import DSXParser
                parser = DSXParser()
                parsed_jobs = parser.parse_multiple_jobs(content, str(dsx_file))
                
                if parsed_jobs and len(parsed_jobs) > 1:
                    # 여러 Job이 포함된 경우 - 각 Job별로 의존성 분석
                    # DSJOB 섹션별로 내용 분리
                    import re
                    dsjob_pattern = r'BEGIN DSJOB\s+(.*?)\s+END DSJOB'
                    dsjob_matches = list(re.finditer(dsjob_pattern, content, re.DOTALL))
                    
                    for i, job_info in enumerate(parsed_jobs):
                        job_name = job_info.get("name")
                        if not job_name:
                            continue
                        
                        # 해당 Job의 내용 추출 (컬럼 정보 추출을 위해 필요)
                        if i < len(dsjob_matches):
                            dsjob_start = dsjob_matches[i].start()
                            if i + 1 < len(dsjob_matches):
                                next_dsjob_start = dsjob_matches[i + 1].start()
                                job_content = content[dsjob_start:next_dsjob_start]
                            else:
                                job_content = content[dsjob_start:]
                        else:
                            job_content = content
                        
                        # parse_multiple_jobs에서 이미 추출한 테이블 정보 사용
                        source_tables = job_info.get("source_tables", [])
                        target_tables = job_info.get("target_tables", [])
                        
                        # 모든 테이블 (스키마 포함)
                        all_tables = []
                        for table in source_tables + target_tables:
                            schema = table.get("schema", "")
                            table_name = table.get("table_name", "")
                            if table_name:
                                full_name = f"{schema}.{table_name}" if schema else table_name
                                all_tables.append({
                                    "full_name": full_name,
                                    "schema": schema,
                                    "table_name": table_name,
                                    "type": table.get("table_type", "unknown"),
                                    "stage_name": table.get("stage_name", ""),
                                    "stage_type": table.get("stage_type", "")
                                })
                        
                        # 파라미터 해석 (옵션)
                        if self.resolve_parameters and self.parameter_mapper:
                            all_tables = self.parameter_mapper.map_tables(all_tables)
                        
                        # 컬럼 정보 추출 (job_content에서)
                        columns = self._extract_columns(job_content)
                        
                        deps = {
                            "job_name": job_name,
                            "file_path": str(dsx_file),
                            "tables": all_tables,
                            "columns": columns,
                            "source_tables": source_tables,
                            "target_tables": target_tables
                        }
                        
                        if deps and deps.get("job_name"):
                            all_dependencies["jobs"].append(deps)
                else:
                    # 단일 Job으로 분석
                    deps = self.analyze_job_dependencies(str(dsx_file))
                    if deps:
                        all_dependencies["jobs"].append(deps)
                    
                    # 테이블별 Job 매핑
                    for table in deps.get("tables", []):
                        full_name = table.get("full_name", "")
                        if full_name:
                            all_dependencies["tables"][full_name].append({
                                "job_name": deps.get("job_name"),
                                "file_path": deps.get("file_path"),
                                "type": table.get("type")
                            })
                    
                    # 컬럼별 Job 매핑
                    for table_name, columns in deps.get("columns", {}).items():
                        for col in columns:
                            col_name = col.get("name", "")
                            if col_name:
                                all_dependencies["columns"][table_name][col_name].append({
                                    "job_name": deps.get("job_name"),
                                    "file_path": deps.get("file_path"),
                                    "type": col.get("type")
                                })
            except Exception as e:
                logger.debug(f"Job 분석 실패: {dsx_file} - {e}")
                continue
        
        logger.info(f"전체 의존성 분석 완료: {len(all_dependencies['jobs'])}개 Job")
        return all_dependencies

