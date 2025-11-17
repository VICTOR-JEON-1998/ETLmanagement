"""여러 Job이 포함된 DSX 파일 파싱 테스트"""

from pathlib import Path
from src.datastage.dsx_parser import DSXParser
from src.core.config import get_config

def test_multiple_jobs_parsing():
    """여러 Job이 포함된 DSX 파일 파싱 테스트"""
    config = get_config()
    ds_config = config.get_datastage_config()
    export_dir = ds_config.get("local_export_path", "")
    
    if not export_dir or not Path(export_dir).exists():
        print(f"Export 디렉토리가 존재하지 않습니다: {export_dir}")
        return
    
    parser = DSXParser()
    
    # DSX 파일 찾기
    export_path = Path(export_dir)
    dsx_files = list(export_path.glob("*.dsx"))
    dsx_files.extend([f for f in export_path.iterdir() if f.is_file() and not f.suffix])
    
    print(f"\n=== 여러 Job 파싱 테스트 ===\n")
    print(f"Export 디렉토리: {export_dir}\n")
    
    for dsx_file in dsx_files[:3]:  # 처음 3개 파일만 테스트
        try:
            with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                first_lines = ''.join(f.readlines()[:5])
                if 'BEGIN HEADER' not in first_lines and 'BEGIN DSJOB' not in first_lines:
                    continue
            
            print(f"파일: {dsx_file.name}")
            
            with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # 여러 Job 파싱
            parsed_jobs = parser.parse_multiple_jobs(content, str(dsx_file))
            
            if parsed_jobs:
                print(f"  → {len(parsed_jobs)}개 Job 발견:")
                for i, job in enumerate(parsed_jobs, 1):
                    job_name = job.get("name", "Unknown")
                    tables = len(job.get("source_tables", [])) + len(job.get("target_tables", []))
                    print(f"    {i}. {job_name} (테이블: {tables}개)")
            else:
                print(f"  → Job을 찾을 수 없습니다")
            
            print()
        
        except Exception as e:
            print(f"  → 오류: {e}\n")
            continue

if __name__ == "__main__":
    test_multiple_jobs_parsing()

