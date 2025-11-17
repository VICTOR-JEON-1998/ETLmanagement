"""메타데이터 통합 관리 및 전파 모듈"""

from typing import Dict, Any, List, Optional, Set
from collections import defaultdict

from src.datastage.api_client import DataStageAPIClient
from src.datastage.job_parser import JobParser
from src.core.logger import get_logger

logger = get_logger(__name__)


class MetadataManager:
    """메타데이터 통합 관리 클래스"""
    
    def __init__(self, api_client: Optional[DataStageAPIClient] = None):
        """
        메타데이터 관리자 초기화
        
        Args:
            api_client: DataStage API 클라이언트 (None이면 새로 생성)
        """
        self.api_client = api_client or DataStageAPIClient()
        self.job_parser = JobParser()
    
    def analyze_impact(
        self,
        project_name: str,
        table_name: str,
        schema: Optional[str] = None,
        column_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        영향도 분석: 특정 컬럼/테이블을 사용 중인 모든 Job 목록 조회
        
        Args:
            project_name: 프로젝트 이름
            table_name: 테이블 이름
            schema: 스키마 이름 (선택)
            column_name: 컬럼 이름 (선택, None이면 테이블 전체)
        
        Returns:
            영향도 분석 결과 딕셔너리
        """
        logger.info(f"영향도 분석 시작: {schema}.{table_name}" + (f".{column_name}" if column_name else ""))
        
        # 전체 Job 목록 조회
        all_jobs = self.api_client.get_jobs(project_name)
        
        impacted_jobs = []
        usage_details = []
        
        for job in all_jobs:
            job_name = job.get("name")
            if not job_name:
                continue
            
            # Job 정의 조회
            job_definition = self.api_client.get_job_definition(project_name, job_name)
            if not job_definition:
                continue
            
            # Job 파싱
            metadata = self.job_parser.parse_job_definition(job_definition)
            
            # 테이블 사용 여부 확인
            is_used, usage_info = self._check_table_usage(
                metadata,
                table_name,
                schema,
                column_name
            )
            
            if is_used:
                impacted_jobs.append({
                    "job_name": job_name,
                    "project": project_name,
                    "usage_type": usage_info.get("usage_type"),  # source, target, both
                    "stages": usage_info.get("stages", []),
                    "columns": usage_info.get("columns", [])
                })
                usage_details.append(usage_info)
        
        result = {
            "table_name": table_name,
            "schema": schema,
            "column_name": column_name,
            "total_jobs_analyzed": len(all_jobs),
            "impacted_jobs_count": len(impacted_jobs),
            "impacted_jobs": impacted_jobs,
            "summary": self._generate_impact_summary(impacted_jobs)
        }
        
        logger.info(f"영향도 분석 완료: {len(impacted_jobs)}개 Job 영향받음")
        return result
    
    def _check_table_usage(
        self,
        metadata: Dict[str, Any],
        table_name: str,
        schema: Optional[str],
        column_name: Optional[str]
    ) -> tuple[bool, Dict[str, Any]]:
        """
        Job에서 테이블/컬럼 사용 여부 확인
        
        Args:
            metadata: 파싱된 Job 메타데이터
            table_name: 테이블 이름
            schema: 스키마 이름
            column_name: 컬럼 이름
        
        Returns:
            (사용 여부, 사용 정보)
        """
        usage_info = {
            "usage_type": None,  # "source", "target", "both"
            "stages": [],
            "columns": []
        }
        
        is_used = False
        used_as_source = False
        used_as_target = False
        
        # Source 테이블 확인
        for source_table in metadata.get("source_tables", []):
            if self._match_table(source_table, table_name, schema):
                used_as_source = True
                usage_info["stages"].append({
                    "stage_name": source_table.get("stage_name"),
                    "stage_type": source_table.get("stage_type"),
                    "role": "source"
                })
                is_used = True
        
        # Target 테이블 확인
        for target_table in metadata.get("target_tables", []):
            if self._match_table(target_table, table_name, schema):
                used_as_target = True
                usage_info["stages"].append({
                    "stage_name": target_table.get("stage_name"),
                    "stage_type": target_table.get("stage_type"),
                    "role": "target"
                })
                is_used = True
        
        # 컬럼 사용 확인 (컬럼이 지정된 경우)
        if column_name and is_used:
            columns_used = self._check_column_usage(metadata, column_name)
            if columns_used:
                usage_info["columns"] = columns_used
            else:
                # 테이블은 사용하지만 해당 컬럼은 사용하지 않음
                is_used = False
        
        # 사용 타입 설정
        if used_as_source and used_as_target:
            usage_info["usage_type"] = "both"
        elif used_as_source:
            usage_info["usage_type"] = "source"
        elif used_as_target:
            usage_info["usage_type"] = "target"
        
        return is_used, usage_info
    
    def _match_table(
        self,
        table_info: Dict[str, Any],
        table_name: str,
        schema: Optional[str]
    ) -> bool:
        """테이블 매칭 확인"""
        table_name_match = table_info.get("table_name", "").lower() == table_name.lower()
        
        if schema:
            schema_match = table_info.get("schema", "").lower() == schema.lower()
            return table_name_match and schema_match
        
        return table_name_match
    
    def _check_column_usage(
        self,
        metadata: Dict[str, Any],
        column_name: str
    ) -> List[Dict[str, Any]]:
        """컬럼 사용 확인"""
        columns_used = []
        
        for link in metadata.get("links", []):
            for column in link.get("columns", []):
                if column.get("name", "").lower() == column_name.lower():
                    columns_used.append({
                        "link_name": link.get("name"),
                        "source_stage": link.get("source_stage"),
                        "target_stage": link.get("target_stage"),
                        "column_info": column
                    })
        
        return columns_used
    
    def _generate_impact_summary(self, impacted_jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """영향도 요약 생성"""
        summary = {
            "total_impacted": len(impacted_jobs),
            "by_usage_type": defaultdict(int),
            "stages_affected": set()
        }
        
        for job in impacted_jobs:
            usage_type = job.get("usage_type", "unknown")
            summary["by_usage_type"][usage_type] += 1
            
            for stage in job.get("stages", []):
                summary["stages_affected"].add(stage.get("stage_name"))
        
        summary["by_usage_type"] = dict(summary["by_usage_type"])
        summary["stages_affected"] = list(summary["stages_affected"])
        
        return summary
    
    def propagate_metadata(
        self,
        project_name: str,
        table_name: str,
        schema: Optional[str],
        column_changes: Dict[str, Any],
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """
        메타데이터 일괄 전파: 컬럼 변경사항을 관련 Job에 자동 반영
        
        Args:
            project_name: 프로젝트 이름
            table_name: 테이블 이름
            schema: 스키마 이름
            column_changes: 변경사항 딕셔너리
                예: {
                    "column_name": "old_column",
                    "new_name": "new_column",  # 이름 변경
                    "new_length": 100,  # 길이 변경
                    "new_type": "VARCHAR"  # 타입 변경
                }
            dry_run: True면 실제 변경 없이 시뮬레이션만 수행
        
        Returns:
            전파 결과 딕셔너리
        """
        logger.info(f"메타데이터 전파 시작: {schema}.{table_name} (dry_run={dry_run})")
        
        # 영향도 분석
        impact_result = self.analyze_impact(
            project_name,
            table_name,
            schema,
            column_changes.get("column_name")
        )
        
        if impact_result["impacted_jobs_count"] == 0:
            logger.warning("영향받는 Job이 없습니다")
            return {
                "success": True,
                "message": "영향받는 Job이 없습니다",
                "jobs_updated": 0
            }
        
        # 각 Job에 변경사항 적용
        updated_jobs = []
        failed_jobs = []
        
        for job_info in impact_result["impacted_jobs"]:
            job_name = job_info["job_name"]
            
            try:
                if not dry_run:
                    # Job 정의 조회
                    job_definition = self.api_client.get_job_definition(project_name, job_name)
                    if not job_definition:
                        failed_jobs.append({
                            "job_name": job_name,
                            "error": "Job 정의를 조회할 수 없습니다"
                        })
                        continue
                    
                    # 메타데이터 수정
                    updated_definition = self._update_job_metadata(
                        job_definition,
                        column_changes,
                        job_info
                    )
                    
                    # Job 정의 업데이트
                    success = self.api_client.update_job_definition(
                        project_name,
                        job_name,
                        updated_definition
                    )
                    
                    if success:
                        updated_jobs.append(job_name)
                        logger.info(f"Job 메타데이터 업데이트 성공: {job_name}")
                    else:
                        failed_jobs.append({
                            "job_name": job_name,
                            "error": "Job 정의 업데이트 실패"
                        })
                else:
                    # Dry run: 실제 변경 없이 시뮬레이션
                    updated_jobs.append(job_name)
                    logger.info(f"[DRY RUN] Job 메타데이터 업데이트 예정: {job_name}")
            
            except Exception as e:
                logger.error(f"Job 메타데이터 업데이트 실패: {job_name} - {e}")
                failed_jobs.append({
                    "job_name": job_name,
                    "error": str(e)
                })
        
        result = {
            "success": len(failed_jobs) == 0,
            "dry_run": dry_run,
            "total_impacted": impact_result["impacted_jobs_count"],
            "jobs_updated": len(updated_jobs),
            "jobs_failed": len(failed_jobs),
            "updated_jobs": updated_jobs,
            "failed_jobs": failed_jobs
        }
        
        logger.info(f"메타데이터 전파 완료: {len(updated_jobs)}개 Job 업데이트")
        return result
    
    def _update_job_metadata(
        self,
        job_definition: Dict[str, Any],
        column_changes: Dict[str, Any],
        job_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Job 메타데이터 수정
        
        Args:
            job_definition: Job 정의
            column_changes: 변경사항
            job_info: Job 사용 정보
        
        Returns:
            수정된 Job 정의
        """
        # Job 정의를 문자열로 변환하여 수정 (실제 구현에서는 더 정교한 파싱 필요)
        import json
        
        job_str = json.dumps(job_definition) if isinstance(job_definition, dict) else str(job_definition)
        
        column_name = column_changes.get("column_name")
        
        # 컬럼 이름 변경
        if "new_name" in column_changes:
            old_name = column_name
            new_name = column_changes["new_name"]
            job_str = job_str.replace(old_name, new_name)
        
        # 컬럼 길이 변경
        if "new_length" in column_changes:
            # 정규식으로 길이 패턴 찾아서 변경 (실제 구현에서는 더 정교한 파싱 필요)
            import re
            pattern = f'"{column_name}"[^}}]*"length"[^,}}]*(\d+)'
            replacement = f'"{column_name}"... "length": {column_changes["new_length"]}'
            job_str = re.sub(pattern, replacement, job_str)
        
        # 수정된 정의 반환
        try:
            return json.loads(job_str)
        except:
            return job_definition  # 파싱 실패 시 원본 반환

