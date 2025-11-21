"""DataStage Job 의존성 그래프 모듈"""

from typing import Dict, Any, List, Optional, Set, Tuple
from collections import defaultdict, deque
from pathlib import Path

from src.core.logger import get_logger

logger = get_logger(__name__)


class DependencyGraph:
    """Job-Table 의존성 그래프 클래스"""
    
    def __init__(self):
        """의존성 그래프 초기화"""
        # Job -> 소스 테이블 매핑
        self.job_to_sources: Dict[str, Set[str]] = defaultdict(set)
        
        # Job -> 타겟 테이블 매핑
        self.job_to_targets: Dict[str, Set[str]] = defaultdict(set)
        
        # 테이블 -> 소스로 사용하는 Job 매핑
        self.table_to_source_jobs: Dict[str, Set[str]] = defaultdict(set)
        
        # 테이블 -> 타겟으로 사용하는 Job 매핑
        self.table_to_target_jobs: Dict[str, Set[str]] = defaultdict(set)
        
        # Job 메타데이터
        self.job_metadata: Dict[str, Dict[str, Any]] = {}
    
    def add_job(
        self,
        job_name: str,
        source_tables: List[Dict[str, Any]],
        target_tables: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Job을 그래프에 추가
        
        Args:
            job_name: Job 이름
            source_tables: 소스 테이블 리스트
            target_tables: 타겟 테이블 리스트
            metadata: Job 메타데이터 (선택)
        """
        # 소스 테이블 정규화
        source_table_names = set()
        for table in source_tables:
            if isinstance(table, dict):
                full_name = table.get("full_name", "")
                # full_name이 없으면 schema와 table_name으로 구성
                if not full_name:
                    schema = table.get("schema", "")
                    table_name = table.get("table_name", "")
                    if table_name:
                        full_name = f"{schema}.{table_name}" if schema else table_name
                if full_name:
                    # 정규화: dbo. 접두사 제거 (MSSQL 기본 스키마 처리)
                    normalized_name = full_name.upper()
                    if normalized_name.startswith("DBO."):
                        normalized_name = normalized_name[4:]
                    source_table_names.add(normalized_name)
            elif isinstance(table, str):
                if table:
                    normalized_name = table.upper()
                    if normalized_name.startswith("DBO."):
                        normalized_name = normalized_name[4:]
                    source_table_names.add(normalized_name)
        
        # 타겟 테이블 정규화
        target_table_names = set()
        for table in target_tables:
            if isinstance(table, dict):
                full_name = table.get("full_name", "")
                # full_name이 없으면 schema와 table_name으로 구성
                if not full_name:
                    schema = table.get("schema", "")
                    table_name = table.get("table_name", "")
                    if table_name:
                        full_name = f"{schema}.{table_name}" if schema else table_name
                if full_name:
                    # 정규화: dbo. 접두사 제거
                    normalized_name = full_name.upper()
                    if normalized_name.startswith("DBO."):
                        normalized_name = normalized_name[4:]
                    target_table_names.add(normalized_name)
            elif isinstance(table, str):
                if table:
                    normalized_name = table.upper()
                    if normalized_name.startswith("DBO."):
                        normalized_name = normalized_name[4:]
                    target_table_names.add(normalized_name)
        
        # 그래프 업데이트
        self.job_to_sources[job_name] = source_table_names
        self.job_to_targets[job_name] = target_table_names
        
        # 역방향 매핑
        for table_name in source_table_names:
            self.table_to_source_jobs[table_name].add(job_name)
        
        for table_name in target_table_names:
            self.table_to_target_jobs[table_name].add(job_name)
        
        # 메타데이터 저장
        if metadata:
            self.job_metadata[job_name] = metadata
        else:
            self.job_metadata[job_name] = {
                "source_tables": source_tables,
                "target_tables": target_tables
            }
    
    def build_from_dependencies(self, all_dependencies: Dict[str, Any]) -> None:
        """
        analyze_all_dependencies 결과로부터 그래프 구축
        
        Args:
            all_dependencies: analyze_all_dependencies()의 결과
        """
        jobs = all_dependencies.get("jobs", [])
        
        for job_dep in jobs:
            job_name = job_dep.get("job_name")
            if not job_name:
                continue
            
            source_tables = job_dep.get("source_tables", [])
            target_tables = job_dep.get("target_tables", [])
            
            # source_tables/target_tables가 비어있으면 tables에서 추출
            if not source_tables and not target_tables:
                all_tables = job_dep.get("tables", [])
                if all_tables:
                    # tables에서 type에 따라 분류
                    for table in all_tables:
                        if isinstance(table, dict):
                            # type 필드 확인 (table_type이 아니라 type)
                            table_type = table.get("type", "").lower()
                            
                            # stage_type으로 판단 (더 정확)
                            stage_type = table.get("stage_type", "").upper()
                            
                            # stage_name으로도 판단 (일반적으로 S_는 소스, T_는 타겟)
                            stage_name = table.get("stage_name", "").upper()
                            
                            is_source = False
                            is_target = False
                            
                            # type 필드로 판단
                            if "source" in table_type:
                                is_source = True
                            elif "target" in table_type:
                                is_target = True
                            # stage_type으로 판단
                            elif any(x in stage_type for x in ["INPUT", "SOURCE", "READ", "CUSTOMINPUT"]):
                                is_source = True
                            elif any(x in stage_type for x in ["OUTPUT", "TARGET", "WRITE", "CUSTOMOUTPUT"]):
                                is_target = True
                            # stage_name으로 판단
                            elif stage_name.startswith("S_") or "SOURCE" in stage_name:
                                is_source = True
                            elif stage_name.startswith("T_") or "TARGET" in stage_name:
                                is_target = True
                            
                            if is_source:
                                source_tables.append(table)
                            elif is_target:
                                target_tables.append(table)
                            else:
                                # 기본적으로 소스로 간주 (대부분의 경우)
                                source_tables.append(table)
                    
                    # 디버깅: 분류 결과 확인
                    if source_tables or target_tables:
                        logger.debug(f"Job '{job_name}': {len(source_tables)}개 소스, {len(target_tables)}개 타겟 테이블")
            
            # 테이블 정보 변환
            source_list = []
            for table in source_tables:
                if isinstance(table, dict):
                    source_list.append(table)
                else:
                    # 문자열인 경우
                    source_list.append({"full_name": table})
            
            target_list = []
            for table in target_tables:
                if isinstance(table, dict):
                    target_list.append(table)
                else:
                    target_list.append({"full_name": table})
            
            self.add_job(job_name, source_list, target_list, job_dep)
        
        logger.info(f"의존성 그래프 구축 완료: {len(self.job_metadata)}개 Job")
    
    def get_direct_impact_jobs(self, table_name: str, schema: Optional[str] = None) -> Set[str]:
        """
        특정 테이블을 직접 사용하는 Job 찾기
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름
        
        Returns:
            Job 이름 집합
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        full_table_name = full_table_name.upper()
        
        # 소스 또는 타겟으로 사용하는 Job
        impacted_jobs = set()
        impacted_jobs.update(self.table_to_source_jobs.get(full_table_name, set()))
        impacted_jobs.update(self.table_to_target_jobs.get(full_table_name, set()))
        
        return impacted_jobs
    
    def get_cascading_impact(
        self,
        table_name: str,
        schema: Optional[str] = None,
        max_level: int = 3
    ) -> Dict[int, Dict[str, Any]]:
        """
        연쇄적 영향도 분석
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름
            max_level: 최대 연쇄 레벨
        
        Returns:
            레벨별 영향도 정보
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        full_table_name = full_table_name.upper()
        
        result = {}
        visited_jobs: Set[str] = set()
        visited_tables: Set[str] = {full_table_name}
        current_level_tables: Set[str] = {full_table_name}
        
        for level in range(max_level + 1):
            level_jobs: Set[str] = set()
            level_tables: Set[str] = set()
            
            # 현재 레벨의 테이블을 사용하는 Job 찾기
            for table in current_level_tables:
                # 소스로 사용하는 Job
                source_jobs = self.table_to_source_jobs.get(table, set())
                # 타겟으로 사용하는 Job
                target_jobs = self.table_to_target_jobs.get(table, set())
                
                for job in source_jobs | target_jobs:
                    if job not in visited_jobs:
                        level_jobs.add(job)
                        visited_jobs.add(job)
                        
                        # 이 Job의 타겟 테이블들을 다음 레벨로
                        target_tables = self.job_to_targets.get(job, set())
                        for target_table in target_tables:
                            if target_table not in visited_tables:
                                level_tables.add(target_table)
                                visited_tables.add(target_table)
            
            if level_jobs or level_tables:
                result[level] = {
                    "jobs": list(level_jobs),
                    "tables": list(level_tables),
                    "job_count": len(level_jobs),
                    "table_count": len(level_tables)
                }
            
            # 다음 레벨을 위한 테이블 업데이트
            current_level_tables = level_tables
            
            # 더 이상 영향이 없으면 종료
            if not level_tables:
                break
        
        return result
    
    def get_table_dependency_chain(
        self,
        start_table: str,
        end_table: Optional[str] = None,
        max_depth: int = 10
    ) -> List[List[str]]:
        """
        테이블 간 의존성 체인 찾기 (BFS)
        
        Args:
            start_table: 시작 테이블
            end_table: 종료 테이블 (None이면 모든 경로)
            max_depth: 최대 깊이
        
        Returns:
            의존성 체인 리스트 (각 체인은 [table1, job1, table2, job2, ...] 형식)
        """
        start_table = start_table.upper()
        if end_table:
            end_table = end_table.upper()
        
        chains = []
        queue = deque([(start_table, [start_table], 0)])
        visited = set()
        
        while queue:
            current_table, path, depth = queue.popleft()
            
            if depth >= max_depth:
                continue
            
            # 현재 테이블을 타겟으로 사용하는 Job 찾기
            target_jobs = self.table_to_target_jobs.get(current_table, set())
            
            for job in target_jobs:
                # 이 Job의 타겟 테이블들
                target_tables = self.job_to_targets.get(job, set())
                
                for next_table in target_tables:
                    # 순환 방지
                    path_key = f"{current_table}->{job}->{next_table}"
                    if path_key in visited:
                        continue
                    visited.add(path_key)
                    
                    new_path = path + [job, next_table]
                    
                    # 종료 테이블에 도달했거나 종료 테이블이 없으면 경로 저장
                    if end_table is None or next_table == end_table:
                        chains.append(new_path)
                    
                    # 다음 레벨 탐색
                    if next_table != end_table:
                        queue.append((next_table, new_path, depth + 1))
        
        return chains
    
    def get_job_dependencies(self, job_name: str) -> Dict[str, Any]:
        """
        특정 Job의 의존성 정보 반환
        
        Args:
            job_name: Job 이름
        
        Returns:
            의존성 정보
        """
        source_tables = list(self.job_to_sources.get(job_name, set()))
        target_tables = list(self.job_to_targets.get(job_name, set()))
        
        # 이 Job의 타겟 테이블을 소스로 사용하는 다른 Job들
        dependent_jobs = set()
        for target_table in target_tables:
            dependent_jobs.update(self.table_to_source_jobs.get(target_table, set()))
        dependent_jobs.discard(job_name)  # 자기 자신 제외
        
        # 이 Job의 소스 테이블을 타겟으로 사용하는 다른 Job들 (선행 Job)
        prerequisite_jobs = set()
        for source_table in source_tables:
            prerequisite_jobs.update(self.table_to_target_jobs.get(source_table, set()))
        prerequisite_jobs.discard(job_name)  # 자기 자신 제외
        
        return {
            "job_name": job_name,
            "source_tables": source_tables,
            "target_tables": target_tables,
            "dependent_jobs": list(dependent_jobs),  # 이 Job 이후 실행되는 Job
            "prerequisite_jobs": list(prerequisite_jobs),  # 이 Job 이전 실행되는 Job
            "metadata": self.job_metadata.get(job_name, {})
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """그래프 통계 반환"""
        total_jobs = len(self.job_metadata)
        total_tables = len(self.table_to_source_jobs | self.table_to_target_jobs)
        
        # 가장 많이 사용되는 테이블
        table_usage_count = defaultdict(int)
        for table in self.table_to_source_jobs.keys():
            table_usage_count[table] += len(self.table_to_source_jobs[table])
        for table in self.table_to_target_jobs.keys():
            table_usage_count[table] += len(self.table_to_target_jobs[table])
        
        most_used_tables = sorted(
            table_usage_count.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        # 가장 많은 테이블을 사용하는 Job
        job_table_count = {
            job: len(sources) + len(targets)
            for job, sources in self.job_to_sources.items()
            for targets in [self.job_to_targets.get(job, set())]
        }
        
        most_complex_jobs = sorted(
            job_table_count.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        return {
            "total_jobs": total_jobs,
            "total_tables": total_tables,
            "most_used_tables": dict(most_used_tables),
            "most_complex_jobs": dict(most_complex_jobs),
            "average_tables_per_job": sum(job_table_count.values()) / total_jobs if total_jobs > 0 else 0
        }
    
    def clear(self) -> None:
        """그래프 초기화"""
        self.job_to_sources.clear()
        self.job_to_targets.clear()
        self.table_to_source_jobs.clear()
        self.table_to_target_jobs.clear()
        self.job_metadata.clear()
        logger.info("의존성 그래프 초기화 완료")

