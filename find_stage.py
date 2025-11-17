"""Stage 찾기 스크립트"""

import re
from pathlib import Path

dsx_file = Path("Datastage export jobs/exportall.dsx")

with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
    content = f.read()

# m_DM_CUST_a01 Job 범위 찾기
job_start = content.find('BEGIN DSJOB')
job_end = content.find('END DSJOB', job_start + 100)

# T_DM_CUST 찾기
pos = content.find('T_DM_CUST', job_start)
if pos > 0:
    # 주변 2000자 출력
    start = max(0, pos - 500)
    end = min(len(content), pos + 1500)
    print("=" * 80)
    print("T_DM_CUST 주변 내용:")
    print("=" * 80)
    print(content[start:end])
    print("=" * 80)
    
    # DSRECORD 패턴 찾기
    # T_DM_CUST 앞에서 가장 가까운 BEGIN DSRECORD 찾기
    before = content[:pos]
    begin_match = list(re.finditer(r'BEGIN DSRECORD', before))[-1] if list(re.finditer(r'BEGIN DSRECORD', before)) else None
    
    if begin_match:
        record_start = begin_match.start()
        # 다음 END DSRECORD 찾기
        after = content[record_start:]
        end_match = re.search(r'END DSRECORD', after)
        if end_match:
            record_end = record_start + end_match.end()
            print("\n" + "=" * 80)
            print("DSRECORD 전체 내용:")
            print("=" * 80)
            print(content[record_start:record_end])
            print("=" * 80)

