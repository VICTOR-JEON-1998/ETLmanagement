"""Export 파일 정보 확인"""

from pathlib import Path
from src.core.config import get_config

def check_files():
    config = get_config()
    ds_config = config.get_datastage_config()
    export_dir = ds_config.get("local_export_path", "")
    
    if not export_dir or not Path(export_dir).exists():
        print(f"Export 디렉토리가 존재하지 않습니다: {export_dir}")
        return
    
    export_path = Path(export_dir)
    dsx_files = list(export_path.glob("*.dsx"))
    dsx_files.extend([f for f in export_path.iterdir() if f.is_file() and not f.suffix])
    
    print(f"Export 디렉토리: {export_dir}\n")
    print(f"총 {len(dsx_files)}개 파일 발견\n")
    
    for dsx_file in dsx_files:
        file_size = dsx_file.stat().st_size
        size_mb = file_size / 1024 / 1024
        print(f"파일: {dsx_file.name}")
        print(f"  크기: {size_mb:.2f} MB ({file_size:,} bytes)")
        
        # 파일의 처음 몇 줄 확인
        try:
            with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                first_lines = [f.readline() for _ in range(5)]
                print(f"  첫 줄: {first_lines[0][:100]}...")
        except:
            print(f"  읽기 실패")
        print()

if __name__ == "__main__":
    check_files()

