"""DataStage Job 메타데이터 인덱스 관리 모듈"""

import json
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from collections import defaultdict

from src.core.logger import get_logger

logger = get_logger(__name__)


class JobIndex:
    """Job 메타데이터 인덱스 관리 클래스"""
    
    def __init__(self, cache_dir: Optional[str] = None):
        """
        인덱스 초기화
        
        Args:
            cache_dir: 캐시 디렉토리 경로 (기본값: 프로젝트 루트/cache)
        """
        if cache_dir is None:
            # 프로젝트 루트의 cache 디렉토리 사용
            project_root = Path(__file__).parent.parent.parent
            cache_dir = project_root / "cache"
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.index_file = self.cache_dir / "job_index.json"
        self.metadata_file = self.cache_dir / "job_metadata.json"
        
        # 인메모리 인덱스
        self._index: Dict[str, Dict[str, Any]] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}
        
        # 로드
        self._load_index()
    
    def _load_index(self) -> None:
        """인덱스 파일 로드"""
        try:
            if self.index_file.exists():
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    self._index = json.load(f)
                logger.info(f"인덱스 로드 완료: {len(self._index)}개 Job")
            
            if self.metadata_file.exists():
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    self._metadata = json.load(f)
                logger.info(f"메타데이터 로드 완료: {len(self._metadata)}개 Job")
        except Exception as e:
            logger.warning(f"인덱스 로드 실패: {e}")
            self._index = {}
            self._metadata = {}
    
    def _save_index(self) -> None:
        """인덱스 파일 저장"""
        try:
            with open(self.index_file, 'w', encoding='utf-8') as f:
                json.dump(self._index, f, indent=2, ensure_ascii=False, default=str)
            
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self._metadata, f, indent=2, ensure_ascii=False, default=str)
            
            logger.debug(f"인덱스 저장 완료: {len(self._index)}개 Job")
        except Exception as e:
            logger.error(f"인덱스 저장 실패: {e}")
    
    def _get_file_hash(self, file_path: Path) -> str:
        """파일 해시 계산 (변경 감지용)"""
        try:
            # 파일 크기와 수정 시간으로 빠른 해시 생성
            stat = file_path.stat()
            content = f"{stat.st_size}_{stat.st_mtime}"
            return hashlib.md5(content.encode()).hexdigest()
        except Exception as e:
            logger.debug(f"파일 해시 계산 실패: {file_path} - {e}")
            return ""
    
    def get_job_key(self, job_name: str, file_path: str) -> str:
        """Job 고유 키 생성"""
        return f"{job_name}::{file_path}"
    
    def is_job_cached(self, job_name: str, file_path: str, file_hash: Optional[str] = None) -> bool:
        """
        Job이 캐시되어 있는지 확인
        
        Args:
            job_name: Job 이름
            file_path: DSX 파일 경로
            file_hash: 파일 해시 (제공 시 변경 감지)
        
        Returns:
            캐시 여부
        """
        job_key = self.get_job_key(job_name, file_path)
        
        if job_key not in self._index:
            return False
        
        if file_hash:
            cached_hash = self._index[job_key].get("file_hash")
            if cached_hash != file_hash:
                return False
        
        return True
    
    def get_cached_job(self, job_name: str, file_path: str) -> Optional[Dict[str, Any]]:
        """
        캐시된 Job 메타데이터 가져오기
        
        Args:
            job_name: Job 이름
            file_path: DSX 파일 경로
        
        Returns:
            Job 메타데이터 또는 None
        """
        job_key = self.get_job_key(job_name, file_path)
        return self._metadata.get(job_key)
    
    def cache_job(
        self,
        job_name: str,
        file_path: str,
        metadata: Dict[str, Any],
        file_hash: Optional[str] = None
    ) -> None:
        """
        Job 메타데이터 캐시
        
        Args:
            job_name: Job 이름
            file_path: DSX 파일 경로
            metadata: Job 메타데이터
            file_hash: 파일 해시
        """
        job_key = self.get_job_key(job_name, file_path)
        
        if file_hash is None:
            file_path_obj = Path(file_path)
            if file_path_obj.exists():
                file_hash = self._get_file_hash(file_path_obj)
        
        # 인덱스 업데이트
        self._index[job_key] = {
            "job_name": job_name,
            "file_path": file_path,
            "file_hash": file_hash,
            "cached_at": datetime.now().isoformat()
        }
        
        # 메타데이터 저장
        self._metadata[job_key] = metadata
        
        logger.debug(f"Job 캐시: {job_name}")
    
    def get_all_cached_jobs(self) -> List[Dict[str, Any]]:
        """모든 캐시된 Job 목록 반환"""
        return list(self._metadata.values())
    
    def get_jobs_by_table(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        특정 테이블을 사용하는 Job 찾기 (캐시에서)
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름
        
        Returns:
            Job 메타데이터 리스트
        """
        matching_jobs = []
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        for job_metadata in self._metadata.values():
            tables = job_metadata.get("tables", [])
            for table in tables:
                table_full = table.get("full_name", "")
                if (table_full.upper() == full_table_name.upper() or
                    table.get("table_name", "").upper() == table_name.upper()):
                    matching_jobs.append(job_metadata)
                    break
        
        return matching_jobs
    
    def get_jobs_by_column(
        self,
        column_name: str,
        table_name: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        특정 컬럼을 사용하는 Job 찾기 (캐시에서)
        
        Args:
            column_name: 컬럼 이름
            table_name: 테이블 이름 (선택)
            schema: 스키마 이름 (선택)
        
        Returns:
            Job 메타데이터 리스트
        """
        matching_jobs = []
        
        for job_metadata in self._metadata.values():
            columns = job_metadata.get("columns", {})
            
            if table_name:
                # 특정 테이블의 컬럼만 확인
                full_table_name = f"{schema}.{table_name}" if schema else table_name
                table_columns = columns.get(full_table_name, [])
                for col in table_columns:
                    if col.get("name", "").upper() == column_name.upper():
                        matching_jobs.append(job_metadata)
                        break
            else:
                # 모든 테이블에서 컬럼 찾기
                for table_cols in columns.values():
                    for col in table_cols:
                        if col.get("name", "").upper() == column_name.upper():
                            matching_jobs.append(job_metadata)
                            break
                    if job_metadata in matching_jobs:
                        break
        
        return matching_jobs
    
    def invalidate_job(self, job_name: str, file_path: str) -> None:
        """
        특정 Job 캐시 무효화
        
        Args:
            job_name: Job 이름
            file_path: DSX 파일 경로
        """
        job_key = self.get_job_key(job_name, file_path)
        
        if job_key in self._index:
            del self._index[job_key]
        
        if job_key in self._metadata:
            del self._metadata[job_key]
        
        logger.debug(f"Job 캐시 무효화: {job_name}")
    
    def invalidate_file(self, file_path: str) -> None:
        """
        특정 파일의 모든 Job 캐시 무효화
        
        Args:
            file_path: DSX 파일 경로
        """
        keys_to_remove = [
            key for key in self._index.keys()
            if self._index[key].get("file_path") == file_path
        ]
        
        for key in keys_to_remove:
            del self._index[key]
            if key in self._metadata:
                del self._metadata[key]
        
        logger.info(f"파일 캐시 무효화: {file_path} ({len(keys_to_remove)}개 Job)")
    
    def clear_cache(self) -> None:
        """전체 캐시 삭제"""
        self._index = {}
        self._metadata = {}
        logger.info("전체 캐시 삭제 완료")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """캐시 통계 반환"""
        total_jobs = len(self._index)
        
        # 테이블별 Job 수
        table_job_count = defaultdict(int)
        for job_metadata in self._metadata.values():
            tables = job_metadata.get("tables", [])
            for table in tables:
                table_full = table.get("full_name", "")
                if table_full:
                    table_job_count[table_full] += 1
        
        # 컬럼별 Job 수
        column_job_count = defaultdict(int)
        for job_metadata in self._metadata.values():
            columns = job_metadata.get("columns", {})
            for table_cols in columns.values():
                for col in table_cols:
                    col_name = col.get("name", "")
                    if col_name:
                        column_job_count[col_name] += 1
        
        return {
            "total_jobs": total_jobs,
            "total_tables": len(table_job_count),
            "total_columns": len(column_job_count),
            "most_used_tables": dict(sorted(table_job_count.items(), key=lambda x: x[1], reverse=True)[:10]),
            "most_used_columns": dict(sorted(column_job_count.items(), key=lambda x: x[1], reverse=True)[:10])
        }
    
    def build_index_from_directory(
        self,
        export_directory: str,
        analyzer: Any,
        force_rebuild: bool = False
    ) -> Dict[str, Any]:
        """
        디렉토리에서 인덱스 구축
        
        Args:
            export_directory: Export 디렉토리 경로
            analyzer: DependencyAnalyzer 인스턴스
            force_rebuild: 강제 재구축 여부
        
        Returns:
            구축 통계
        """
        directory = Path(export_directory)
        if not directory.exists():
            logger.error(f"디렉토리가 존재하지 않습니다: {export_directory}")
            return {}
        
        stats = {
            "total_files": 0,
            "processed_files": 0,
            "cached_jobs": 0,
            "skipped_jobs": 0,
            "errors": 0
        }
        
        # DSX 파일 찾기
        dsx_files = list(directory.glob("*.dsx"))
        dsx_files.extend([f for f in directory.iterdir() if f.is_file() and not f.suffix])
        
        # 하위 디렉토리도 스캔
        for subdir in directory.iterdir():
            if subdir.is_dir():
                dsx_files.extend(list(subdir.glob("*.dsx")))
        
        stats["total_files"] = len(dsx_files)
        
        logger.info(f"인덱스 구축 시작: {len(dsx_files)}개 파일")
        
        for dsx_file in dsx_files:
            try:
                file_hash = self._get_file_hash(dsx_file)
                
                # 파일이 캐시되어 있고 강제 재구축이 아니면 스킵
                if not force_rebuild and self.is_job_cached("", str(dsx_file), file_hash):
                    # 파일 해시가 같으면 스킵 (단, 여러 Job이 포함된 경우는 확인 필요)
                    continue
                
                # 파일 읽기
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    first_lines = ''.join(f.readlines()[:5])
                    if 'BEGIN HEADER' not in first_lines and 'BEGIN DSJOB' not in first_lines:
                        continue
                
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                # 여러 Job 파싱
                from src.datastage.dsx_parser import DSXParser
                parser = DSXParser()
                parsed_jobs = parser.parse_multiple_jobs(content, str(dsx_file))
                
                if not parsed_jobs:
                    continue
                
                stats["processed_files"] += 1
                
                # 각 Job 분석 및 캐시
                import re
                dsjob_pattern = r'BEGIN DSJOB\s+(.*?)\s+END DSJOB'
                dsjob_matches = list(re.finditer(dsjob_pattern, content, re.DOTALL))
                
                for i, job_info in enumerate(parsed_jobs):
                    job_name = job_info.get("name") or job_info.get("identifier", "Unknown")
                    
                    # Job 내용 추출
                    if i < len(dsjob_matches):
                        dsjob_start = dsjob_matches[i].start()
                        if i + 1 < len(dsjob_matches):
                            next_dsjob_start = dsjob_matches[i + 1].start()
                            job_content = content[dsjob_start:next_dsjob_start]
                        else:
                            job_content = content[dsjob_start:]
                    else:
                        job_content = content
                    
                    # 의존성 분석
                    try:
                        deps = analyzer.analyze_job_dependencies(str(dsx_file), job_content)
                        
                        # 메타데이터 구성
                        metadata = {
                            "job_name": deps.get("job_name", job_name),
                            "file_path": str(dsx_file),
                            "tables": deps.get("tables", []),
                            "columns": deps.get("columns", {}),
                            "source_tables": deps.get("source_tables", []),
                            "target_tables": deps.get("target_tables", [])
                        }
                        
                        # 캐시 저장
                        self.cache_job(job_name, str(dsx_file), metadata, file_hash)
                        stats["cached_jobs"] += 1
                    except Exception as e:
                        logger.debug(f"Job 분석 실패: {job_name} - {e}")
                        stats["errors"] += 1
                        stats["skipped_jobs"] += 1
                
            except Exception as e:
                logger.debug(f"파일 처리 실패: {dsx_file} - {e}")
                stats["errors"] += 1
                continue
        
        # 인덱스 저장
        self._save_index()
        
        logger.info(f"인덱스 구축 완료: {stats['cached_jobs']}개 Job 캐시됨")
        return stats

