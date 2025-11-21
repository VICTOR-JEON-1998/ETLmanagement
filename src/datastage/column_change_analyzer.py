"""컬럼 변경 영향도 분석 및 변경 가이드 생성 모듈"""

from typing import Dict, Any, List, Optional, Set
from pathlib import Path
from collections import defaultdict

from src.core.logger import get_logger

logger = get_logger(__name__)


class ColumnChangeAnalyzer:
    """컬럼 변경 영향도 분석 및 변경 가이드 생성 클래스"""
    
    def __init__(self, dependency_analyzer):
        """
        초기화
        
        Args:
            dependency_analyzer: DependencyAnalyzer 인스턴스
        """
        self.analyzer = dependency_analyzer
    
    def analyze_column_change(
        self,
        column_name: str,
        change_type: str = "rename",
        new_name: Optional[str] = None,
        export_directory: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        컬럼 변경 영향도 분석 및 변경 가이드 생성
        
        Args:
            column_name: 변경할 컬럼명
            change_type: 변경 유형 ("rename", "delete", "modify", "add")
            new_name: 새 컬럼명 (rename 시)
            export_directory: Export 디렉토리
        
        Returns:
            영향도 분석 결과 및 변경 가이드
        """
        logger.info(f"컬럼 변경 영향도 분석 시작: {column_name} ({change_type})")
        
        directory = Path(export_directory) if export_directory else self.analyzer.export_directory
        if not directory or not directory.exists():
            logger.warning(f"Export 디렉토리가 존재하지 않습니다: {directory}")
            return {}
        
        # 1. 컬럼을 포함하는 테이블 찾기
        tables_with_column = self.analyzer.find_tables_using_column(
            column_name, str(directory)
        )
        
        # 2. 컬럼을 사용하는 모든 Job 찾기
        jobs_with_column = self.analyzer.find_jobs_using_column_only(
            column_name, str(directory)
        )
        
        # 3. 테이블별 Job 그룹화
        table_to_jobs = defaultdict(list)
        job_to_tables = defaultdict(set)
        
        for table_info in tables_with_column:
            full_name = table_info.get("full_name", "")
            related_jobs = table_info.get("related_jobs", [])
            for job_info in related_jobs:
                job_name = job_info.get("job_name")
                if job_name:
                    table_to_jobs[full_name].append({
                        "job_name": job_name,
                        "file_path": job_info.get("file_path", "")
                    })
                    job_to_tables[job_name].add(full_name)
        
        # 4. Job별 변경 가이드 생성
        job_change_guides = []
        for job in jobs_with_column:
            job_name = job.get("job_name")
            file_path = job.get("file_path", "")
            all_tables = job.get("all_tables", [])
            
            # Job에서 사용하는 테이블 중 해당 컬럼이 있는 테이블
            affected_tables = job_to_tables.get(job_name, set())
            
            change_guide = {
                "job_name": job_name,
                "file_path": file_path,
                "file_name": Path(file_path).name if file_path else "",
                "affected_tables": list(affected_tables),
                "change_actions": self._generate_change_actions(
                    column_name, change_type, new_name, affected_tables
                )
            }
            job_change_guides.append(change_guide)
        
        # 5. 결과 구성
        result = {
            "column_name": column_name,
            "change_type": change_type,
            "new_name": new_name,
            "summary": {
                "total_tables": len(tables_with_column),
                "total_jobs": len(jobs_with_column),
                "unique_tables": len(table_to_jobs),
                "unique_jobs": len(set(job.get("job_name") for job in jobs_with_column))
            },
            "tables": [
                {
                    "full_name": t.get("full_name"),
                    "schema": t.get("schema"),
                    "table_name": t.get("table_name"),
                    "job_count": t.get("job_count", 0),
                    "related_jobs": [
                        {
                            "job_name": j.get("job_name"),
                            "file_path": j.get("file_path")
                        }
                        for j in t.get("related_jobs", [])
                    ]
                }
                for t in tables_with_column
            ],
            "jobs": job_change_guides,
            "change_guide": self._generate_overall_guide(
                column_name, change_type, new_name, tables_with_column, jobs_with_column
            )
        }
        
        logger.info(f"컬럼 변경 영향도 분석 완료: {len(tables_with_column)}개 테이블, {len(jobs_with_column)}개 Job")
        return result
    
    def _generate_change_actions(
        self,
        column_name: str,
        change_type: str,
        new_name: Optional[str],
        affected_tables: Set[str]
    ) -> List[str]:
        """Job별 변경 작업 목록 생성"""
        actions = []
        
        if change_type == "rename":
            if new_name:
                actions.append(f"컬럼명 '{column_name}' → '{new_name}' 변경")
                actions.append(f"DSX 파일에서 '{column_name}' 검색하여 '{new_name}'로 일괄 변경")
            else:
                actions.append(f"컬럼명 '{column_name}' 변경 (새 이름 미지정)")
        elif change_type == "delete":
            actions.append(f"컬럼 '{column_name}' 삭제")
            actions.append(f"DSX 파일에서 '{column_name}' 관련 코드 제거")
        elif change_type == "modify":
            actions.append(f"컬럼 '{column_name}' 타입/속성 수정")
            actions.append(f"DSX 파일에서 '{column_name}' 관련 타입/속성 확인 및 수정")
        elif change_type == "add":
            actions.append(f"컬럼 '{column_name}' 추가")
            actions.append(f"DSX 파일에서 '{column_name}' 추가 로직 구현")
        
        if affected_tables:
            actions.append(f"영향받는 테이블: {', '.join(list(affected_tables)[:3])}{'...' if len(affected_tables) > 3 else ''}")
        
        actions.append("Job 재배포 및 테스트")
        
        return actions
    
    def _generate_overall_guide(
        self,
        column_name: str,
        change_type: str,
        new_name: Optional[str],
        tables: List[Dict[str, Any]],
        jobs: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """전체 변경 가이드 생성"""
        guide = {
            "overview": f"컬럼 '{column_name}' {self._get_change_type_korean(change_type)} 영향도 분석",
            "affected_scope": {
                "tables": len(tables),
                "jobs": len(jobs)
            },
            "steps": [
                "1. 영향받는 테이블 목록 확인",
                "2. 각 테이블의 스키마 변경 (필요시)",
                "3. 영향받는 Job 목록 확인",
                "4. 각 Job의 DSX 파일 수정",
                "5. Job 재배포 및 테스트",
                "6. 연쇄 영향도 확인 (다운스트림 Job)"
            ],
            "detailed_steps": []
        }
        
        # 상세 단계
        if change_type == "rename" and new_name:
            guide["detailed_steps"].append({
                "step": 1,
                "description": f"DSX 파일에서 '{column_name}' 검색",
                "action": f"모든 DSX 파일에서 '{column_name}' 문자열 검색"
            })
            guide["detailed_steps"].append({
                "step": 2,
                "description": f"컬럼명 '{column_name}' → '{new_name}' 변경",
                "action": f"검색된 모든 위치에서 '{column_name}'를 '{new_name}'로 일괄 변경"
            })
        elif change_type == "delete":
            guide["detailed_steps"].append({
                "step": 1,
                "description": f"컬럼 '{column_name}' 사용 위치 확인",
                "action": f"DSX 파일에서 '{column_name}' 사용 위치 모두 확인"
            })
            guide["detailed_steps"].append({
                "step": 2,
                "description": f"컬럼 '{column_name}' 관련 코드 제거",
                "action": f"SELECT, JOIN, WHERE 등에서 '{column_name}' 참조 제거"
            })
        
        guide["detailed_steps"].append({
            "step": len(guide["detailed_steps"]) + 1,
            "description": "Job 재배포",
            "action": "수정된 DSX 파일을 DataStage에 재배포"
        })
        
        guide["detailed_steps"].append({
            "step": len(guide["detailed_steps"]) + 1,
            "description": "테스트",
            "action": "각 Job 실행하여 정상 작동 확인"
        })
        
        return guide
    
    def _get_change_type_korean(self, change_type: str) -> str:
        """변경 유형 한글 변환"""
        mapping = {
            "rename": "이름 변경",
            "delete": "삭제",
            "modify": "수정",
            "add": "추가"
        }
        return mapping.get(change_type, change_type)

