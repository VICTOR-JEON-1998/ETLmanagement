"""DataStage Job 의존성 분석 모듈"""

import re
from typing import Dict, Any, List, Optional, Set, Tuple
from pathlib import Path
from collections import defaultdict
from datetime import datetime

from src.core.logger import get_logger
from src.datastage.dsx_parser import DSXParser
from src.datastage.parameter_mapper import ParameterMapper
from src.datastage.job_index import JobIndex
from src.datastage.dependency_graph import DependencyGraph
try:
    from src.database.connectors import get_connector
except Exception:  # pragma: no cover  (database 모듈이 없는 경량화 환경)
    get_connector = None

logger = get_logger(__name__)


class DependencyAnalyzer:
    """DataStage Job 의존성 분석 클래스"""
    
    def __init__(self, export_directory: Optional[str] = None, resolve_parameters: bool = False, use_cache: bool = True):
        """
        의존성 분석기 초기화
        
        Args:
            export_directory: Export된 DSX 파일이 있는 디렉토리
            resolve_parameters: 파라미터를 실제 DB 정보로 해석할지 여부
            use_cache: 캐시 사용 여부
        """
        self.dsx_parser = DSXParser()
        self.export_directory = Path(export_directory) if export_directory else None
        self._job_cache: Dict[str, Dict[str, Any]] = {}
        self.resolve_parameters = resolve_parameters
        self.parameter_mapper = ParameterMapper() if resolve_parameters else None
        self.use_cache = use_cache
        self.job_index = JobIndex() if use_cache else None
        self.dependency_graph: Optional[DependencyGraph] = None
    
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
    
    def _get_dsx_files_hybrid(self, directory: Path) -> List[Path]:
        """
        하이브리드 방식으로 DSX 파일 목록 가져오기
        우선순위: exportall.dsx → jobs/ 디렉토리 → 기타 파일
        
        Args:
            directory: Export 디렉토리
        
        Returns:
            DSX 파일 경로 리스트
        """
        dsx_files = []
        
        # 1. exportall.dsx 우선 확인
        exportall_file = directory / "exportall.dsx"
        if exportall_file.exists():
            dsx_files.append(exportall_file)
            logger.debug(f"exportall.dsx 발견: {exportall_file}")
        
        # 2. jobs/ 디렉토리 확인
        jobs_dir = directory / "jobs"
        if jobs_dir.exists() and jobs_dir.is_dir():
            job_files = list(jobs_dir.glob("*.dsx"))
            dsx_files.extend(job_files)
            logger.debug(f"jobs/ 디렉토리에서 {len(job_files)}개 파일 발견")
        
        # 3. 기타 DSX 파일 (exportall.dsx가 없거나 jobs/ 디렉토리가 없는 경우)
        if not exportall_file.exists():
            other_files = list(directory.glob("*.dsx"))
            other_files.extend([f for f in directory.iterdir() if f.is_file() and not f.suffix])
            # exportall.dsx와 jobs/ 디렉토리 파일 제외
            dsx_files.extend([f for f in other_files if f not in dsx_files])
        
        return dsx_files
    
    def find_jobs_using_table(self, table_name: str, schema: Optional[str] = None,
                              export_directory: Optional[str] = None, use_cache: Optional[bool] = None) -> List[Dict[str, Any]]:
        """
        특정 테이블을 사용하는 Job 찾기 (하이브리드 스캔 + 캐시 활용)
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름 (선택)
            export_directory: Export 디렉토리 (None이면 초기화 시 설정한 값 사용)
            use_cache: 캐시 사용 여부 (None이면 초기화 시 설정 사용)
        
        Returns:
            해당 테이블을 사용하는 Job 리스트
        """
        directory = Path(export_directory) if export_directory else self.export_directory
        if not directory or not directory.exists():
            logger.warning(f"Export 디렉토리가 존재하지 않습니다: {directory}")
            return []
        
        use_cache_flag = use_cache if use_cache is not None else self.use_cache
        
        # 캐시에서 먼저 확인
        if use_cache_flag and self.job_index:
            cached_jobs = self.job_index.get_jobs_by_table(table_name, schema)
            if cached_jobs:
                logger.info(f"캐시에서 {len(cached_jobs)}개 Job 발견")
                matching_jobs = []
                for job_meta in cached_jobs:
                    matching_jobs.append({
                        "job_name": job_meta.get("job_name"),
                        "file_path": job_meta.get("file_path"),
                        "table_usage": {"table_name": table_name, "schema": schema},
                        "all_tables": job_meta.get("tables", [])
                    })
                return matching_jobs
        
        matching_jobs = []
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        # 하이브리드 스캔: exportall.dsx 우선, jobs/ 디렉토리 보조
        dsx_files = self._get_dsx_files_hybrid(directory)
        
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
                               export_directory: Optional[str] = None,
                               use_cache: Optional[bool] = None) -> List[Dict[str, Any]]:
        """
        특정 컬럼을 사용하는 Job 찾기 (하이브리드 스캔 + 캐시 활용)
        
        Args:
            table_name: 테이블 이름
            column_name: 컬럼 이름
            schema: 스키마 이름 (선택)
            export_directory: Export 디렉토리
            use_cache: 캐시 사용 여부
        
        Returns:
            해당 컬럼을 사용하는 Job 리스트
        """
        directory = Path(export_directory) if export_directory else self.export_directory
        if not directory or not directory.exists():
            logger.warning(f"Export 디렉토리가 존재하지 않습니다: {directory}")
            return []
        
        use_cache_flag = use_cache if use_cache is not None else self.use_cache
        
        # 캐시에서 먼저 확인
        if use_cache_flag and self.job_index:
            cached_jobs = self.job_index.get_jobs_by_column(column_name, table_name, schema)
            if cached_jobs:
                logger.info(f"캐시에서 {len(cached_jobs)}개 Job 발견")
                matching_jobs = []
                for job_meta in cached_jobs:
                    matching_jobs.append({
                        "job_name": job_meta.get("job_name"),
                        "file_path": job_meta.get("file_path"),
                        "column_usage": {"table_name": table_name, "column_name": column_name},
                        "table_name": f"{schema}.{table_name}" if schema else table_name,
                        "all_columns": job_meta.get("columns", {}).get(f"{schema}.{table_name}" if schema else table_name, [])
                    })
                return matching_jobs
        
        matching_jobs = []
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        # 하이브리드 스캔: exportall.dsx 우선, jobs/ 디렉토리 보조
        dsx_files = self._get_dsx_files_hybrid(directory)
        
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
                
                # 정규화: dbo. 제거
                normalized_full_name = table_full_name.upper()
                if normalized_full_name.startswith("DBO."):
                    normalized_full_name = normalized_full_name[4:]
                
                if normalized_full_name not in tables_dict:
                    # 테이블 정보 파싱
                    schema = None
                    table_name = normalized_full_name
                    if "." in normalized_full_name:
                        parts = normalized_full_name.split(".", 1)
                        schema = parts[0]
                        table_name = parts[1]
                    
                    tables_dict[normalized_full_name] = {
                        "table_name": table_name,
                        "schema": schema,
                        "full_name": normalized_full_name,
                        "column_name": column_name,
                        "related_jobs": [],
                        "job_count": 0
                    }
                
                # Job 추가 (중복 제거)
                job_info = {"job_name": job_name, "file_path": file_path}
                if job_info not in tables_dict[normalized_full_name]["related_jobs"]:
                    tables_dict[normalized_full_name]["related_jobs"].append(job_info)
                    tables_dict[normalized_full_name]["job_count"] += 1
        
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
    
    def build_cache_index(self, export_directory: Optional[str] = None, force_rebuild: bool = False) -> Dict[str, Any]:
        """
        캐시 인덱스 구축
        
        Args:
            export_directory: Export 디렉토리
            force_rebuild: 강제 재구축 여부
        
        Returns:
            구축 통계
        """
        if not self.job_index:
            logger.warning("캐시가 비활성화되어 있습니다.")
            return {}
        
        directory = Path(export_directory) if export_directory else self.export_directory
        if not directory or not directory.exists():
            logger.warning(f"Export 디렉토리가 존재하지 않습니다: {directory}")
            return {}
        
        logger.info("캐시 인덱스 구축 시작...")
        stats = self.job_index.build_index_from_directory(str(directory), self, force_rebuild)
        return stats
    
    def build_dependency_graph(self, export_directory: Optional[str] = None) -> DependencyGraph:
        """
        의존성 그래프 구축
        
        Args:
            export_directory: Export 디렉토리
        
        Returns:
            DependencyGraph 인스턴스
        """
        logger.info("의존성 그래프 구축 시작...")
        
        # 전체 의존성 분석
        all_dependencies = self.analyze_all_dependencies(export_directory)
        
        # 그래프 구축
        graph = DependencyGraph()
        graph.build_from_dependencies(all_dependencies)
        
        self.dependency_graph = graph
        logger.info("의존성 그래프 구축 완료")
        return graph
    
    def analyze_cascading_impact(
        self,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        schema: Optional[str] = None,
        change_type: str = "modify",
        new_name: Optional[str] = None,
        export_directory: Optional[str] = None,
        max_level: int = 3
    ) -> Dict[str, Any]:
        """
        연쇄적 영향도 분석
        
        Args:
            table_name: 테이블 이름 (컬럼 분석 시 필수)
            column_name: 컬럼 이름 (선택)
            schema: 스키마 이름 (선택)
            change_type: 변경 유형 ("rename", "delete", "modify", "add")
            new_name: 새 이름 (rename 시)
            export_directory: Export 디렉토리
            max_level: 최대 연쇄 레벨
        
        Returns:
            연쇄 영향도 분석 결과
        """
        if not table_name and not column_name:
            raise ValueError("table_name 또는 column_name 중 하나는 필수입니다.")
        
        # 의존성 그래프 구축 (없는 경우)
        if not self.dependency_graph:
            self.build_dependency_graph(export_directory)
        
        graph = self.dependency_graph
        if not graph:
            logger.warning("의존성 그래프가 구축되지 않았습니다.")
            return {}
        
        # 직접 영향 Job 찾기
        if column_name:
            # 컬럼을 사용하는 Job 찾기
            if table_name:
                direct_jobs = self.find_jobs_using_column(table_name, column_name, schema, export_directory)
            else:
                direct_jobs = self.find_jobs_using_column_only(column_name, export_directory)
            
            direct_job_names = {job.get("job_name") for job in direct_jobs if job.get("job_name")}
            
            # 컬럼이 사용되는 테이블들
            affected_tables = set()
            for job in direct_jobs:
                all_tables = job.get("all_tables", [])
                for table in all_tables:
                    if isinstance(table, str):
                        affected_tables.add(table.upper())
                    elif isinstance(table, dict):
                        full_name = table.get("full_name", "")
                        if full_name:
                            affected_tables.add(full_name.upper())
            
            # 각 테이블에 대해 연쇄 분석
            cascading_results = {}
            for table in affected_tables:
                if table_name and schema:
                    table_parts = table.split(".")
                    if len(table_parts) == 2:
                        table_schema, table_name_only = table_parts
                        if table_schema.upper() != schema.upper() or table_name_only.upper() != table_name.upper():
                            continue
                
                cascading = graph.get_cascading_impact(
                    table_name=table.split(".")[-1] if "." in table else table,
                    schema=table.split(".")[0] if "." in table and len(table.split(".")) == 2 else None,
                    max_level=max_level
                )
                cascading_results[table] = cascading
            
            # 모든 레벨 통합
            all_levels = {}
            for level in range(max_level + 1):
                level_jobs = set()
                level_tables = set()
                
                for table, cascading in cascading_results.items():
                    if level in cascading:
                        level_jobs.update(cascading[level]["jobs"])
                        level_tables.update(cascading[level]["tables"])
                
                if level_jobs or level_tables:
                    all_levels[level] = {
                        "jobs": list(level_jobs),
                        "tables": list(level_tables),
                        "job_count": len(level_jobs),
                        "table_count": len(level_tables)
                    }
        else:
            # 테이블만 분석
            # 그래프에 파라미터 형태의 테이블명이 저장되어 있을 수 있으므로
            # find_jobs_using_table을 사용하여 직접 영향 Job 찾기
            direct_jobs = self.find_jobs_using_table(table_name, schema, export_directory)
            direct_job_names = {job.get("job_name") for job in direct_jobs if job.get("job_name")}
            
            # 직접 영향 Job들의 타겟 테이블을 찾아서 연쇄 분석
            all_levels = {}
            if direct_job_names:
                # Level 0: 직접 영향
                all_levels[0] = {
                    "jobs": list(direct_job_names),
                    "tables": set(),
                    "job_count": len(direct_job_names),
                    "table_count": 0
                }
                
                # 직접 영향 Job들의 타겟 테이블 수집
                target_tables = set()
                for job in direct_jobs:
                    all_tables = job.get("all_tables", [])
                    for table in all_tables:
                        if isinstance(table, dict):
                            table_type = table.get("type", "").lower()
                            if "target" in table_type:
                                full_name = table.get("full_name", "")
                                if full_name:
                                    target_tables.add(full_name.upper())
                
                # Level 1 이상: 타겟 테이블을 소스로 사용하는 Job 찾기
                if target_tables and max_level > 0:
                    visited_jobs = set(direct_job_names)
                    current_level_jobs = set(direct_job_names)
                    
                    for level in range(1, max_level + 1):
                        next_level_jobs = set()
                        next_level_tables = set()
                        
                        for job_name in current_level_jobs:
                            if job_name in graph.job_metadata:
                                job_meta = graph.job_metadata[job_name]
                                job_targets = job_meta.get("target_tables", [])
                                
                                for target_table in job_targets:
                                    if isinstance(target_table, dict):
                                        full_name = target_table.get("full_name", "")
                                        if full_name:
                                            # 이 테이블을 소스로 사용하는 Job 찾기
                                            source_jobs = graph.table_to_source_jobs.get(full_name.upper(), set())
                                            for next_job in source_jobs:
                                                if next_job not in visited_jobs:
                                                    next_level_jobs.add(next_job)
                                                    visited_jobs.add(next_job)
                                            next_level_tables.add(full_name.upper())
                        
                        if next_level_jobs or next_level_tables:
                            all_levels[level] = {
                                "jobs": list(next_level_jobs),
                                "tables": list(next_level_tables),
                                "job_count": len(next_level_jobs),
                                "table_count": len(next_level_tables)
                            }
                            current_level_jobs = next_level_jobs
                        else:
                            break
        
        # 결과 구성
        result = {
            "change_type": change_type,
            "table_name": table_name,
            "column_name": column_name,
            "schema": schema,
            "new_name": new_name,
            "direct_impact": {
                "jobs": list(direct_job_names),
                "job_count": len(direct_job_names)
            },
            "cascading_impact": all_levels,
            "summary": {
                "total_impacted_jobs": len(direct_job_names),
                "total_impacted_tables": 0,
                "max_level": max(all_levels.keys()) if all_levels else 0
            }
        }
        
        # 요약 통계
        all_impacted_jobs = set(direct_job_names)
        all_impacted_tables = set()
        
        for level_data in all_levels.values():
            all_impacted_jobs.update(level_data["jobs"])
            all_impacted_tables.update(level_data["tables"])
        
        result["summary"]["total_impacted_jobs"] = len(all_impacted_jobs)
        result["summary"]["total_impacted_tables"] = len(all_impacted_tables)
        
        logger.info(
            f"연쇄 영향도 분석 완료: 직접 {len(direct_job_names)}개 Job, "
            f"총 {len(all_impacted_jobs)}개 Job 영향 (최대 {result['summary']['max_level']}단계)"
        )
        
        return result
    
    def get_pk_info(self, table_name: str, schema: str = "dbo", db_type: str = "mssql") -> Dict[str, Any]:
        """
        테이블의 PK 정보 수집
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름
            db_type: 데이터베이스 타입 ("mssql" 또는 "vertica")
        
        Returns:
            PK 정보 딕셔너리
        """
        if get_connector is None:
            logger.warning("DB 커넥터 모듈이 비활성화되어 PK 정보를 조회할 수 없습니다.")
            return {
                "table_name": table_name,
                "schema": schema,
                "pk_columns": [],
                "pk_column_details": [],
                "has_pk": False,
                "error": "database connectors not available",
            }
        try:
            connector = get_connector(db_type)
            table_schema = connector.get_table_schema(table_name, schema)
            
            pk_columns = [col for col in table_schema if col.get("is_pk")]
            
            result = {
                "table_name": table_name,
                "schema": schema,
                "pk_columns": [col["name"] for col in pk_columns],
                "pk_column_details": pk_columns,
                "has_pk": len(pk_columns) > 0
            }
            
            logger.info(f"PK 정보 수집 완료: {schema}.{table_name} - {len(pk_columns)}개 PK 컬럼")
            return result
        except Exception as e:
            logger.error(f"PK 정보 수집 실패: {schema}.{table_name} - {e}")
            return {
                "table_name": table_name,
                "schema": schema,
                "pk_columns": [],
                "pk_column_details": [],
                "has_pk": False,
                "error": str(e)
            }
    
    def analyze_pk_impact(
        self,
        table_name: str,
        schema: str = "dbo",
        old_pk: Optional[List[str]] = None,
        new_pk: Optional[List[str]] = None,
        db_type: str = "mssql",
        export_directory: Optional[str] = None,
        max_level: int = 3
    ) -> Dict[str, Any]:
        """
        PK 변경 영향도 분석
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름
            old_pk: 기존 PK 컬럼 리스트 (None이면 DB에서 조회)
            new_pk: 새 PK 컬럼 리스트
            db_type: 데이터베이스 타입
            export_directory: Export 디렉토리
            max_level: 최대 연쇄 레벨
        
        Returns:
            PK 변경 영향도 분석 결과
        """
        logger.info(f"PK 변경 영향도 분석 시작: {schema}.{table_name}")
        
        # PK 정보 수집
        if old_pk is None:
            pk_info = self.get_pk_info(table_name, schema, db_type)
            old_pk = pk_info.get("pk_columns", [])
        
        if not old_pk:
            logger.warning(f"PK가 없거나 조회할 수 없습니다: {schema}.{table_name}")
            return {
                "table_name": table_name,
                "schema": schema,
                "old_pk": [],
                "new_pk": new_pk or [],
                "error": "PK 정보를 찾을 수 없습니다."
            }
        
        # PK 컬럼을 사용하는 Job 찾기
        pk_jobs = set()
        for pk_column in old_pk:
            jobs = self.find_jobs_using_column(table_name, pk_column, schema, export_directory)
            pk_jobs.update(job.get("job_name") for job in jobs if job.get("job_name"))
        
        # 연쇄 영향도 분석 (PK 컬럼 변경으로 인한)
        cascading_impact = {}
        if pk_jobs:
            # 의존성 그래프 구축 (없는 경우)
            if not self.dependency_graph:
                self.build_dependency_graph(export_directory)
            
            if self.dependency_graph:
                cascading = self.dependency_graph.get_cascading_impact(
                    table_name, schema, max_level
                )
                cascading_impact = cascading
        
        # FK 참조 분석 (다른 테이블에서 이 테이블의 PK를 FK로 사용)
        fk_references = self._find_fk_references(table_name, schema, old_pk, db_type)
        
        # JOIN 조건에서 PK 사용 여부 (DSX에서 추출)
        join_usage = self._find_pk_in_joins(table_name, schema, old_pk, export_directory)
        
        # 결과 구성
        all_impacted_jobs = set(pk_jobs)
        for level_data in cascading_impact.values():
            all_impacted_jobs.update(level_data.get("jobs", []))
        
        result = {
            "table_name": table_name,
            "schema": schema,
            "old_pk": old_pk,
            "new_pk": new_pk or [],
            "direct_impact": {
                "jobs": list(pk_jobs),
                "job_count": len(pk_jobs)
            },
            "cascading_impact": cascading_impact,
            "fk_references": fk_references,
            "join_usage": join_usage,
            "summary": {
                "total_impacted_jobs": len(all_impacted_jobs),
                "fk_referencing_tables": len(fk_references.get("referencing_tables", [])),
                "join_using_jobs": len(join_usage.get("jobs", []))
            }
        }
        
        logger.info(
            f"PK 변경 영향도 분석 완료: 직접 {len(pk_jobs)}개 Job, "
            f"FK 참조 {len(fk_references.get('referencing_tables', []))}개 테이블, "
            f"JOIN 사용 {len(join_usage.get('jobs', []))}개 Job"
        )
        
        return result
    
    def _find_fk_references(
        self,
        table_name: str,
        schema: str,
        pk_columns: List[str],
        db_type: str = "mssql"
    ) -> Dict[str, Any]:
        """
        FK 참조 찾기 (다른 테이블에서 이 테이블의 PK를 참조)
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름
            pk_columns: PK 컬럼 리스트
            db_type: 데이터베이스 타입
        
        Returns:
            FK 참조 정보
        """
        if get_connector is None:
            return {"referencing_tables": [], "references": []}
        try:
            connector = get_connector(db_type)
            
            if db_type == "mssql":
                query = """
                SELECT 
                    fk.TABLE_SCHEMA AS referencing_schema,
                    fk.TABLE_NAME AS referencing_table,
                    fk.COLUMN_NAME AS referencing_column,
                    pk.TABLE_SCHEMA AS referenced_schema,
                    pk.TABLE_NAME AS referenced_table,
                    pk.COLUMN_NAME AS referenced_column
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
                    ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
                    AND rc.CONSTRAINT_SCHEMA = fk.CONSTRAINT_SCHEMA
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
                    ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
                    AND rc.UNIQUE_CONSTRAINT_SCHEMA = pk.CONSTRAINT_SCHEMA
                WHERE pk.TABLE_SCHEMA = ? AND pk.TABLE_NAME = ?
                    AND pk.COLUMN_NAME IN ({})
                """.format(','.join(['?' for _ in pk_columns]))
                
                with connector.connect() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, [schema, table_name] + pk_columns)
                    
                    references = []
                    referencing_tables = set()
                    for row in cursor.fetchall():
                        ref_schema, ref_table, ref_col, _, _, _ = row
                        references.append({
                            "referencing_schema": ref_schema,
                            "referencing_table": ref_table,
                            "referencing_column": ref_col
                        })
                        referencing_tables.add(f"{ref_schema}.{ref_table}")
            
            elif db_type == "vertica":
                query = """
                SELECT 
                    fk.table_schema AS referencing_schema,
                    fk.table_name AS referencing_table,
                    fk.column_name AS referencing_column
                FROM v_catalog.foreign_keys fk
                WHERE fk.reference_table_schema = :schema
                    AND fk.reference_table_name = :table_name
                """
                
                with connector.connect() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, {"schema": schema, "table_name": table_name})
                    
                    references = []
                    referencing_tables = set()
                    for row in cursor.fetchall():
                        ref_schema, ref_table, ref_col = row
                        references.append({
                            "referencing_schema": ref_schema,
                            "referencing_table": ref_table,
                            "referencing_column": ref_col
                        })
                        referencing_tables.add(f"{ref_schema}.{ref_table}")
            else:
                return {"referencing_tables": [], "references": []}
            
            return {
                "referencing_tables": list(referencing_tables),
                "references": references,
                "count": len(referencing_tables)
            }
        except Exception as e:
            logger.warning(f"FK 참조 조회 실패: {e}")
            return {"referencing_tables": [], "references": [], "error": str(e)}
    
    def _find_pk_in_joins(
        self,
        table_name: str,
        schema: str,
        pk_columns: List[str],
        export_directory: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        JOIN 조건에서 PK 사용 여부 찾기
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름
            pk_columns: PK 컬럼 리스트
            export_directory: Export 디렉토리
        
        Returns:
            JOIN 사용 정보
        """
        # PK 컬럼을 사용하는 Job 찾기 (JOIN 조건에서 사용될 가능성)
        join_jobs = []
        
        for pk_column in pk_columns:
            jobs = self.find_jobs_using_column(table_name, pk_column, schema, export_directory)
            for job in jobs:
                job_name = job.get("job_name")
                if job_name and job_name not in [j.get("job_name") for j in join_jobs]:
                    join_jobs.append({
                        "job_name": job_name,
                        "file_path": job.get("file_path"),
                        "column": pk_column,
                        "note": "JOIN 조건에서 사용될 가능성 (DSX에서 정확한 JOIN 정보 추출 필요)"
                    })
        
        return {
            "jobs": join_jobs,
            "count": len(join_jobs)
        }
    
    def comprehensive_impact_analysis(
        self,
        change_type: str,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        schema: Optional[str] = None,
        old_pk: Optional[List[str]] = None,
        new_pk: Optional[List[str]] = None,
        new_name: Optional[str] = None,
        db_type: str = "mssql",
        export_directory: Optional[str] = None,
        max_level: int = 3
    ) -> Dict[str, Any]:
        """
        통합 영향도 분석 (모든 분석 유형 지원)
        
        Args:
            change_type: 변경 유형 ("column_rename", "column_delete", "column_modify", 
                                   "pk_change", "table_rename", "table_delete")
            table_name: 테이블 이름
            column_name: 컬럼 이름 (컬럼 변경 시)
            schema: 스키마 이름
            old_pk: 기존 PK 컬럼 리스트 (PK 변경 시)
            new_pk: 새 PK 컬럼 리스트 (PK 변경 시)
            new_name: 새 이름 (rename 시)
            db_type: 데이터베이스 타입
            export_directory: Export 디렉토리
            max_level: 최대 연쇄 레벨
        
        Returns:
            통합 영향도 분석 결과
        """
        logger.info(f"통합 영향도 분석 시작: {change_type}")
        
        result = {
            "change_type": change_type,
            "table_name": table_name,
            "column_name": column_name,
            "schema": schema,
            "analysis_timestamp": datetime.now().isoformat(),
            "direct_impact": {},
            "cascading_impact": {},
            "summary": {}
        }
        
        try:
            if change_type == "pk_change":
                # PK 변경 분석
                if not table_name:
                    raise ValueError("PK 변경 분석에는 table_name이 필수입니다.")
                
                pk_result = self.analyze_pk_impact(
                    table_name=table_name,
                    schema=schema or "dbo",
                    old_pk=old_pk,
                    new_pk=new_pk,
                    db_type=db_type,
                    export_directory=export_directory,
                    max_level=max_level
                )
                
                result.update({
                    "old_pk": pk_result.get("old_pk", []),
                    "new_pk": pk_result.get("new_pk", []),
                    "direct_impact": pk_result.get("direct_impact", {}),
                    "cascading_impact": pk_result.get("cascading_impact", {}),
                    "fk_references": pk_result.get("fk_references", {}),
                    "join_usage": pk_result.get("join_usage", {}),
                    "summary": pk_result.get("summary", {})
                })
            
            elif change_type in ["column_rename", "column_delete", "column_modify", "column_add"]:
                # 컬럼 변경 분석
                if not table_name and not column_name:
                    raise ValueError("컬럼 변경 분석에는 table_name 또는 column_name이 필수입니다.")
                
                cascading_result = self.analyze_cascading_impact(
                    table_name=table_name,
                    column_name=column_name,
                    schema=schema,
                    change_type=change_type,
                    new_name=new_name,
                    export_directory=export_directory,
                    max_level=max_level
                )
                
                result.update({
                    "new_name": new_name,
                    "direct_impact": cascading_result.get("direct_impact", {}),
                    "cascading_impact": cascading_result.get("cascading_impact", {}),
                    "summary": cascading_result.get("summary", {})
                })
            
            elif change_type in ["table_rename", "table_delete"]:
                # 테이블 변경 분석
                if not table_name:
                    raise ValueError("테이블 변경 분석에는 table_name이 필수입니다.")
                
                cascading_result = self.analyze_cascading_impact(
                    table_name=table_name,
                    schema=schema,
                    change_type=change_type,
                    new_name=new_name,
                    export_directory=export_directory,
                    max_level=max_level
                )
                
                result.update({
                    "new_name": new_name,
                    "direct_impact": cascading_result.get("direct_impact", {}),
                    "cascading_impact": cascading_result.get("cascading_impact", {}),
                    "summary": cascading_result.get("summary", {})
                })
            
            else:
                raise ValueError(f"지원하지 않는 변경 유형: {change_type}")
            
            # 최종 요약 통계
            all_jobs = set(result.get("direct_impact", {}).get("jobs", []))
            for level_data in result.get("cascading_impact", {}).values():
                all_jobs.update(level_data.get("jobs", []))
            
            result["summary"]["total_impacted_jobs"] = len(all_jobs)
            result["summary"]["all_impacted_jobs"] = list(all_jobs)
            
            logger.info(f"통합 영향도 분석 완료: {result['summary'].get('total_impacted_jobs', 0)}개 Job 영향")
            
        except Exception as e:
            logger.error(f"통합 영향도 분석 실패: {e}")
            result["error"] = str(e)
            import traceback
            result["traceback"] = traceback.format_exc()
        
        return result

