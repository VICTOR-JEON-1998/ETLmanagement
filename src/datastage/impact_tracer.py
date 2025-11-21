from typing import List, Dict, Any, Set
from src.datastage.dependency_analyzer import DependencyAnalyzer
from src.core.logger import get_logger

logger = get_logger(__name__)

class ImpactTracer:
    """
    컬럼 변경에 따른 연쇄적인 영향도를 분석하는 클래스
    Flow: Column -> Table -> Job -> Target Table -> Next Job ...
    """
    
    def __init__(self, analyzer: DependencyAnalyzer):
        self.analyzer = analyzer
        if not self.analyzer.graph:
            self.analyzer.build_dependency_graph()
            
    def trace_impact(self, column_name: str, max_depth: int = 3) -> Dict[str, Any]:
        """
        컬럼 변경 시 영향도 추적
        
        Returns:
            {
                "column": column_name,
                "initial_tables": [],
                "impact_chain": [
                    {
                        "level": 1,
                        "source_table": "...",
                        "job": "...",
                        "target_table": "..."
                    },
                    ...
                ]
            }
        """
        # 1. 컬럼을 포함하는 초기 테이블 찾기
        initial_tables_info = self.analyzer.find_tables_using_column(column_name)
        initial_tables = [t['full_name'] for t in initial_tables_info]
        
        impact_chain = []
        visited_tables = set(initial_tables)
        visited_jobs = set()
        
        current_level_tables = initial_tables
        
        for level in range(1, max_depth + 1):
            next_level_tables = set()
            
            for table in current_level_tables:
                # 이 테이블을 Source로 사용하는 Job 찾기
                jobs_using_table = self.analyzer.find_jobs_using_table(
                    table_name=table.split(".")[-1] if "." in table else table,
                    schema=table.split(".")[0] if "." in table else None
                )
                
                for job in jobs_using_table:
                    job_name = job['job_name']
                    
                    if job_name in visited_jobs:
                        continue
                    visited_jobs.add(job_name)
                    
                    # 이 Job의 Target Table 찾기
                    targets = self.analyzer.graph.job_to_targets.get(job_name, [])
                    
                    for target in targets:
                        impact_chain.append({
                            "level": level,
                            "source_table": table,
                            "job": job_name,
                            "target_table": target,
                            "file_path": job.get('file_path')
                        })
                        
                        if target not in visited_tables:
                            visited_tables.add(target)
                            next_level_tables.add(target)
            
            if not next_level_tables:
                break
                
            current_level_tables = list(next_level_tables)
            
        return {
            "column": column_name,
            "initial_tables": initial_tables,
            "impact_chain": impact_chain
        }
