from __future__ import annotations

import json
import csv
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from src.core.config import get_config
from src.core.logger import get_logger


logger = get_logger(__name__)


class ERPImpactAnalyzer:
    """
    ERP → Vertica OD → Vertica FT 영향도 분석기.

    1차 연관: ERP 테이블을 읽고 OD 테이블을 적재하는 Job
    2차 연관: 위 Job들이 만든 OD 테이블을 읽고 FT 테이블을 적재하는 Job
    """

    def __init__(
        self,
        dependency_analyzer,
        export_directory: Optional[str] = None,
        erp_tables: Optional[Set[str]] = None,
    ) -> None:
        self.dependency_analyzer = dependency_analyzer
        self.export_directory = Path(export_directory) if export_directory else dependency_analyzer.export_directory
        self.erp_tables: Set[str] = set()
        self.erp_tables_simple: Set[str] = set()
        self.erp_column_map: Dict[str, Set[str]] = {}
        if erp_tables:
            normalized_tables = {self._normalize_table_name(t) for t in erp_tables}
            self.erp_tables = normalized_tables
            self.erp_tables_simple = {self._strip_schema(t) for t in normalized_tables}

        config = get_config()
        impact_cfg = config.get("erp_impact", {})
        self.od_schemas = {s.upper() for s in impact_cfg.get("od_schemas", [])}
        self.ft_schemas = {s.upper() for s in impact_cfg.get("ft_schemas", [])}
        self.od_prefixes = [p.upper() for p in impact_cfg.get("od_prefixes", [])]
        self.ft_prefixes = [p.upper() for p in impact_cfg.get("ft_prefixes", [])]

        self._job_metadata_cache: Optional[Dict[str, Dict[str, any]]] = None

    # ------------------------------------------------------------------ ERP 테이블 로드
    def load_erp_tables_from_file(self, file_path: str) -> None:
        """파일(한 줄당 하나)에서 ERP 테이블 목록을 로드"""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"ERP 테이블 파일을 찾을 수 없습니다: {file_path}")

        tables: Set[str] = set()
        column_map: Dict[str, Set[str]] = {}

        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                table_entry = row[0].strip()
                if not table_entry or table_entry.startswith("#"):
                    continue
                normalized_table = self._normalize_table_name(table_entry)
                tables.add(normalized_table)

                if len(row) > 1:
                    column_name = row[1].strip()
                    if column_name:
                        column_upper = column_name.upper()
                        column_map.setdefault(column_upper, set()).add(normalized_table)

        if not tables:
            logger.warning("ERP 테이블 리스트가 비어 있습니다.")
        self.erp_tables = tables
        self.erp_tables_simple = {self._strip_schema(t) for t in tables}
        self.erp_column_map = column_map
        logger.info(f"ERP 테이블 {len(self.erp_tables)}개 로드 완료.")
        if self.erp_column_map:
            logger.info(f"ERP 컬럼-테이블 매핑 {len(self.erp_column_map)}개 컬럼 로드.")

    # ------------------------------------------------------------------ 공개 API
    def analyze_column(self, column_name: str, max_level: int = 2) -> Dict[str, any]:
        """
        컬럼명을 기준으로 ERP → OD → FT 영향도를 분석합니다.
        Returns:
            {
               "column": ...,
               "erp_tables": [...],
               "tier1_jobs": [...],
               "tier2_jobs": [...],
               "summary": {...}
            }
        """
        if not self.erp_tables:
            raise ValueError("ERP 테이블 목록이 비어 있습니다. 먼저 load_erp_tables_from_file()을 호출하세요.")

        if not self.export_directory or not self.export_directory.exists():
            raise ValueError("유효한 Export 디렉토리가 필요합니다.")

        logger.info(f"ERP 컬럼 영향도 분석 시작: column={column_name}")

        column_upper = column_name.upper()
        allowed_erp_tables = self.erp_column_map.get(column_upper)
        if allowed_erp_tables:
            logger.info(
                "컬럼 '%s'은 ERP 테이블 %d개에 존재합니다.",
                column_name,
                len(allowed_erp_tables),
            )
        else:
            allowed_erp_tables = self.erp_tables or None
            if allowed_erp_tables is None:
                raise ValueError("ERP 테이블 목록이 비어 있습니다. load_erp_tables_from_file()을 호출하세요.")
            logger.warning(
                "컬럼 '%s'이 ERP 리스트에서 직접 매핑되지 않아 전체 ERP 테이블 %d개를 사용합니다.",
                column_name,
                len(allowed_erp_tables),
            )

        allowed_full = set(allowed_erp_tables)
        allowed_simple = {self._strip_schema(t) for t in allowed_full}
        allowed_tuple = (allowed_full, allowed_simple)

        jobs_with_column = self.dependency_analyzer.find_jobs_using_column_only(
            column_name=column_name,
            export_directory=str(self.export_directory),
        )
        job_meta = self._get_job_metadata()

        tier1_jobs, tier1_od_targets, impacted_erp_tables = self._find_tier1_jobs(
            jobs_with_column,
            job_meta,
            allowed_tuple,
        )
        tier2_jobs = self._find_tier2_jobs(job_meta, tier1_od_targets, max_level)

        result = {
            "column": column_name,
            "erp_tables": sorted(impacted_erp_tables),
            "tier1_jobs": tier1_jobs,
            "tier2_jobs": tier2_jobs,
            "summary": {
                "jobs_with_column": len(jobs_with_column),
                "tier1_jobs": len(tier1_jobs),
                "tier2_jobs": len(tier2_jobs),
                "impacted_erp_tables": len(impacted_erp_tables),
                "candidate_erp_tables": len(allowed_full),
            },
        }
        logger.info(
            "ERP 컬럼 영향도 분석 완료: tier1=%s, tier2=%s",
            len(tier1_jobs),
            len(tier2_jobs),
        )
        return result

    # ------------------------------------------------------------------ 내부 로직
    def _find_tier1_jobs(
        self,
        jobs_with_column: List[Dict[str, any]],
        job_meta: Dict[str, Dict[str, any]],
        allowed_erp_tables: Optional[Tuple[Set[str], Set[str]]],
    ) -> Tuple[List[Dict[str, any]], Set[str], Set[str]]:
        tier1_jobs: List[Dict[str, any]] = []
        tier1_od_targets: Set[str] = set()
        impacted_erp_tables: Set[str] = set()

        for job_entry in jobs_with_column:
            job_name = job_entry.get("job_name")
            metadata = job_meta.get(job_name)
            if not metadata:
                continue

            source_entries = self._get_tables_by_role(metadata, "source")
            target_entries = self._get_tables_by_role(metadata, "target")

            erp_sources = self._collect_tables(
                source_entries,
                desired_type="erp",
                allowed_tables=allowed_erp_tables,
            )
            od_targets = self._collect_tables(target_entries, desired_type="od")
            if not erp_sources:
                erp_sources = self._collect_from_table_names(
                    job_entry.get("all_tables", []),
                    desired_type="erp",
                    allowed_tables=allowed_erp_tables,
                )
            if not od_targets:
                od_targets = self._collect_from_table_names(
                    job_entry.get("all_tables", []),
                    desired_type="od",
                )

            if erp_sources and od_targets:
                tier1_jobs.append(
                    {
                        "job_name": job_name,
                        "file_path": metadata.get("file_path"),
                        "erp_sources": sorted(erp_sources),
                        "od_targets": sorted(od_targets),
                    }
                )
                tier1_od_targets.update(od_targets)
                impacted_erp_tables.update(erp_sources)

        return tier1_jobs, tier1_od_targets, impacted_erp_tables

    def _find_tier2_jobs(
        self,
        job_meta: Dict[str, Dict[str, any]],
        tier1_od_targets: Set[str],
        max_level: int,
    ) -> List[Dict[str, any]]:
        if not tier1_od_targets or max_level < 2:
            return []

        tier2_jobs: List[Dict[str, any]] = []
        tier1_od_set = {t.upper() for t in tier1_od_targets}

        for metadata in job_meta.values():
            job_name = metadata.get("job_name")
            source_entries = self._get_tables_by_role(metadata, "source")
            target_entries = self._get_tables_by_role(metadata, "target")

            od_sources = self._collect_tables(source_entries, desired_type="od")
            if not od_sources or not (od_sources & tier1_od_set):
                continue
            ft_targets = self._collect_tables(target_entries, desired_type="ft")
            if not ft_targets:
                continue

            tier2_jobs.append(
                {
                    "job_name": job_name,
                    "file_path": metadata.get("file_path"),
                    "od_sources": sorted(od_sources),
                    "ft_targets": sorted(ft_targets),
                }
            )

        return tier2_jobs

    def _collect_tables(
        self,
        table_list: List[Dict[str, any]],
        desired_type: str,
        allowed_tables: Optional[Tuple[Set[str], Set[str]]] = None,
    ) -> Set[str]:
        collected: Set[str] = set()
        for table in table_list:
            full_name = table.get("full_name") or self._combine_table(table)
            if not full_name:
                continue
            table_type = self._classify_table(full_name)
            if table_type == desired_type:
                normalized = self._normalize_table_name(full_name)
                if desired_type == "erp" and allowed_tables is not None:
                    allowed_full, allowed_simple = allowed_tables
                    if (
                        normalized not in allowed_full
                        and self._strip_schema(normalized) not in allowed_simple
                    ):
                        continue
                collected.add(normalized)
        return collected

    def _collect_from_table_names(
        self,
        table_names: List[str],
        desired_type: str,
        allowed_tables: Optional[Tuple[Set[str], Set[str]]] = None,
    ) -> Set[str]:
        if not table_names:
            return set()
        pseudo_entries = [{"full_name": name} for name in table_names if name]
        return self._collect_tables(pseudo_entries, desired_type, allowed_tables)

    def _classify_table(self, full_name: str) -> str:
        normalized = self._normalize_table_name(full_name)
        if normalized in self.erp_tables or self._strip_schema(normalized) in self.erp_tables_simple:
            return "erp"

        schema, table = self._split_table(normalized)
        if self._matches(schema, table, self.od_schemas, self.od_prefixes):
            return "od"
        if self._matches(schema, table, self.ft_schemas, self.ft_prefixes):
            return "ft"
        return "other"

    @staticmethod
    def _matches(schema: str, table: str, schema_set: Set[str], prefixes: List[str]) -> bool:
        schema_match = schema in schema_set if schema_set else False
        prefix_match = any(table.startswith(prefix) for prefix in prefixes)
        return schema_match or prefix_match

    def _get_tables_by_role(self, metadata: Dict[str, any], role: str) -> List[Dict[str, any]]:
        role = role.lower()
        explicit = metadata.get(f"{role}_tables") or []
        if explicit:
            return explicit
        tables = metadata.get("tables", [])
        if not tables:
            return []
        result = []
        for table in tables:
            if self._determine_role(table) == role:
                result.append(table)
        return result

    @staticmethod
    def _determine_role(table: Dict[str, any]) -> str:
        table_type = (table.get("type") or "").lower()
        if table_type in {"source", "target"}:
            return table_type

        stage_type = (table.get("stage_type") or "").lower()
        if any(keyword in stage_type for keyword in ("input", "source", "read")):
            return "source"
        if any(keyword in stage_type for keyword in ("output", "target", "write")):
            return "target"

        stage_name = (table.get("stage_name") or "").upper()
        if stage_name.startswith(("S_", "L_", "SRC", "READ")):
            return "source"
        if stage_name.startswith(("T_", "W_", "TRG", "TGT")):
            return "target"

        return "unknown"

    def _get_job_metadata(self) -> Dict[str, Dict[str, any]]:
        if self._job_metadata_cache is not None:
            return self._job_metadata_cache

        dependencies = self.dependency_analyzer.analyze_all_dependencies(str(self.export_directory))
        jobs = dependencies.get("jobs", [])
        metadata = {}
        for job in jobs:
            job_name = job.get("job_name")
            if not job_name:
                continue
            metadata[job_name] = job
        self._job_metadata_cache = metadata
        logger.info(f"Job 메타데이터 {len(metadata)}개 로드")
        return metadata

    # ------------------------------------------------------------------ 유틸리티
    @staticmethod
    def _combine_table(table_info: Dict[str, any]) -> str:
        schema = table_info.get("schema", "")
        table_name = table_info.get("table_name", "")
        if schema and table_name:
            return f"{schema}.{table_name}"
        return table_name or schema

    @staticmethod
    def _split_table(full_name: str) -> Tuple[str, str]:
        if "." in full_name:
            parts = full_name.split(".", 1)
            return parts[0], parts[1]
        return "", full_name

    @staticmethod
    def _normalize_table_name(full_name: str) -> str:
        normalized = full_name.strip().upper()
        if "." not in normalized:
            return normalized
        schema, table = normalized.split(".", 1)
        schema = schema.strip().strip('"')
        table = table.strip().strip('"')
        return f"{schema}.{table}"

    @staticmethod
    def _strip_schema(full_name: str) -> str:
        if "." in full_name:
            return full_name.split(".", 1)[1]
        return full_name

    @staticmethod
    def _strip_schema(full_name: str) -> str:
        if "." in full_name:
            return full_name.split(".", 1)[1]
        return full_name


