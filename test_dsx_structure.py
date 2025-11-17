"""DSX 파일 구조 확인 스크립트"""

from pathlib import Path
from src.core.config import get_config
import re

def check_dsx_structure():
    """DSX 파일 구조 확인"""
    config = get_config()
    ds_config = config.get_datastage_config()
    export_dir = ds_config.get("local_export_path", "")
    
    if not export_dir or not Path(export_dir).exists():
        print(f"Export 디렉토리가 존재하지 않습니다: {export_dir}")
        return
    
    export_path = Path(export_dir)
    dsx_files = list(export_path.glob("*.dsx"))
    dsx_files.extend([f for f in export_path.iterdir() if f.is_file() and not f.suffix])
    
    if not dsx_files:
        print("DSX 파일을 찾을 수 없습니다.")
        return
    
    # STYL_CD를 포함하는 파일 찾기
    print("=== STYL_CD를 포함하는 파일 검색 ===\n")
    styl_files = []
    for dsx_file in dsx_files[:10]:  # 처음 10개만 확인
        try:
            with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                content_sample = f.read(100000)  # 처음 100KB만 확인
                if 'STYL_CD' in content_sample.upper():
                    styl_files.append(dsx_file)
                    print(f"  발견: {dsx_file.name}")
        except:
            continue
    
    if not styl_files:
        print("STYL_CD를 포함하는 파일을 찾지 못했습니다. 모든 파일 검색 중...")
        for dsx_file in dsx_files:
            try:
                with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
                    if 'STYL_CD' in f.read().upper():
                        styl_files.append(dsx_file)
                        print(f"  발견: {dsx_file.name}")
                        break
            except:
                continue
    
    if not styl_files:
        print("STYL_CD를 포함하는 파일을 찾을 수 없습니다.")
        dsx_file = dsx_files[0] if dsx_files else None
    else:
        dsx_file = styl_files[0]
    
    if not dsx_file:
        return
    
    print(f"\n=== DSX 파일 구조 확인: {dsx_file.name} ===\n")
    
    with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # STYL_CD 검색
    print("=== STYL_CD 검색 ===\n")
    styl_cd_matches = list(re.finditer(r'STYL_CD', content, re.IGNORECASE))
    print(f"STYL_CD 발견: {len(styl_cd_matches)}회")
    
    if styl_cd_matches:
        # 처음 5개 매치 주변 컨텍스트 확인
        for i, match in enumerate(styl_cd_matches[:5], 1):
            start = max(0, match.start() - 200)
            end = min(len(content), match.end() + 200)
            context = content[start:end]
            print(f"\n--- 매치 {i} (위치: {match.start()}) ---")
            print(context.replace('\n', ' ')[:400])
            print("...")
    
    # Column 패턴 검색
    print("\n\n=== Column 패턴 검색 ===\n")
    column_patterns = [
        (r'Column\s+"([^"]+)"', 'Column "..."'),
        (r'Column\s+"([^"]+)"\s+Type', 'Column "..." Type'),
        (r'"([A-Z_]+)"', '일반 문자열 패턴'),
    ]
    
    for pattern, desc in column_patterns:
        matches = list(re.finditer(pattern, content))
        print(f"{desc}: {len(matches)}회 발견")
        if matches:
            # STYL_CD와 관련된 매치 찾기
            styl_related = [m for m in matches if 'STYL' in m.group(0).upper()]
            if styl_related:
                print(f"  → STYL 관련: {len(styl_related)}회")
                for m in styl_related[:3]:
                    start = max(0, m.start() - 100)
                    end = min(len(content), m.end() + 100)
                    context = content[start:end].replace('\n', ' ')
                    print(f"    {context[:200]}...")
    
    # DSRECORD 구조 확인
    print("\n\n=== DSRECORD 구조 확인 ===\n")
    record_pattern = r'BEGIN DSRECORD\s+Identifier\s+"([^"]+)"(.*?)END DSRECORD'
    records = list(re.finditer(record_pattern, content, re.DOTALL))
    print(f"DSRECORD 수: {len(records)}개")
    
    # STYL_CD를 포함하는 DSRECORD 찾기
    styl_records = []
    for record in records:
        if 'STYL_CD' in record.group(0).upper():
            styl_records.append(record)
    
    print(f"STYL_CD를 포함하는 DSRECORD: {len(styl_records)}개")
    if styl_records:
        record = styl_records[0]
        record_content = record.group(0)
        print(f"\n첫 번째 DSRECORD (길이: {len(record_content)}):")
        print(record_content[:1000])
        print("...")

if __name__ == "__main__":
    check_dsx_structure()

