"""STYL_CD 검색 디버깅"""

from pathlib import Path
from src.datastage.dependency_analyzer import DependencyAnalyzer
from src.core.config import get_config
import re

def test_styl_cd_search():
    """STYL_CD 검색 테스트"""
    config = get_config()
    ds_config = config.get_datastage_config()
    export_dir = ds_config.get("local_export_path", "")
    
    if not export_dir or not Path(export_dir).exists():
        print(f"Export 디렉토리가 존재하지 않습니다: {export_dir}")
        return
    
    export_path = Path(export_dir)
    print(f"Export 디렉토리: {export_dir}\n")
    
    # 1. 파일에서 STYL_CD 직접 검색
    print("=== 1. 파일에서 STYL_CD 직접 검색 ===\n")
    dsx_files = list(export_path.glob("*.dsx"))
    dsx_files.extend([f for f in export_path.iterdir() if f.is_file() and not f.suffix])
    
    styl_files = []
    print(f"총 {len(dsx_files)}개 파일 검색 중...\n")
    
    # 전체 파일 검색 (큰 파일도 처리)
    for i, dsx_file in enumerate(dsx_files):
        if i % 10 == 0:
            print(f"  진행 중... {i}/{len(dsx_files)}")
        try:
            # 파일 크기 확인
            file_size = dsx_file.stat().st_size
            if file_size > 50 * 1024 * 1024:  # 50MB 이상이면 샘플만
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    # 처음과 끝 부분만 확인
                    head = f.read(1000000)  # 처음 1MB
                    f.seek(max(0, file_size - 1000000))  # 끝 1MB
                    tail = f.read(1000000)
                    content = head + tail
            else:
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
            
            # 다양한 패턴으로 검색
            patterns = [
                (r'STYL_CD', 'STYL_CD'),
                (r'styl_cd', 'styl_cd'),
                (r'Styl_Cd', 'Styl_Cd'),
                (r'STYLCD', 'STYLCD'),
                (r'stylcd', 'stylcd'),
            ]
            
            for pattern, desc in patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    count = len(matches)
                    styl_files.append((dsx_file, count, desc))
                    print(f"  발견: {dsx_file.name} ({desc}: {count}회)")
                    break  # 하나 찾으면 다음 파일로
        except Exception as e:
            continue
    
    print()  # 빈 줄
    
    print(f"\n총 {len(styl_files)}개 파일에서 STYL_CD 발견\n")
    
    # 2. DependencyAnalyzer로 검색
    print("=== 2. DependencyAnalyzer로 검색 ===\n")
    analyzer = DependencyAnalyzer(export_directory=str(export_path))
    
    # find_jobs_using_column_only
    print("find_jobs_using_column_only 실행 중...")
    jobs = analyzer.find_jobs_using_column_only("STYL_CD", export_directory=str(export_path))
    print(f"결과: {len(jobs)}개 Job 발견")
    for job in jobs[:5]:
        print(f"  - {job.get('job_name')} (테이블: {job.get('table_name')})")
    
    # find_tables_using_column
    print("\nfind_tables_using_column 실행 중...")
    tables = analyzer.find_tables_using_column("STYL_CD", export_directory=str(export_path))
    print(f"결과: {len(tables)}개 테이블 발견")
    for tbl in tables[:5]:
        print(f"  - {tbl.get('full_name')} (Job: {tbl.get('job_count')}개)")
    
    # 3. 실제 파일 내용 확인
    if styl_files:
        print(f"\n=== 3. 실제 파일 내용 확인: {styl_files[0][0].name} ===\n")
        with open(styl_files[0][0], 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # STYL_CD 주변 컨텍스트
        matches = list(re.finditer(r'STYL_CD', content, re.IGNORECASE))
        print(f"STYL_CD 발견: {len(matches)}회\n")
        
        for i, match in enumerate(matches[:3], 1):
            start = max(0, match.start() - 300)
            end = min(len(content), match.end() + 300)
            context = content[start:end]
            print(f"--- 매치 {i} (위치: {match.start()}) ---")
            # 줄바꿈을 공백으로 변환하여 가독성 향상
            context_lines = context.split('\n')
            print(' '.join(context_lines[:5]))
            print("...")

if __name__ == "__main__":
    test_styl_cd_search()

