"""DSX 파일 상세 분석 스크립트"""

import sys
import re
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.core.config import get_config
from src.core.logger import setup_logger, get_logger
from src.datastage.dsx_parser import DSXParser
from src.datastage.dependency_analyzer import DependencyAnalyzer

# 로거 설정
config = get_config()
logging_config = config.get_logging_config()
setup_logger(
    "etlmanagement",
    level="DEBUG",
    log_format=logging_config.get("format"),
    log_file=logging_config.get("file")
)
logger = get_logger(__name__)

def analyze_job_detail():
    """특정 Job의 상세 정보 분석"""
    
    dsx_file = Path("Datastage export jobs/exportall.dsx")
    
    if not dsx_file.exists():
        print(f"DSX 파일을 찾을 수 없습니다: {dsx_file}")
        return
    
    print(f"DSX 파일 분석: {dsx_file}\n")
    print("=" * 80)
    
    # 파일 읽기
    with open(dsx_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # m_DM_CUST_a01 Job 찾기
    job_pattern = r'BEGIN DSJOB\s+(.*?)\s+END DSJOB'
    dsjob_matches = list(re.finditer(job_pattern, content, re.DOTALL))
    
    target_job_content = None
    for match in dsjob_matches:
        dsjob_content = match.group(1)
        if 'Identifier "m_DM_CUST_a01"' in dsjob_content:
            # 다음 DSJOB까지 또는 파일 끝까지
            start_pos = match.start()
            if match == dsjob_matches[-1]:
                target_job_content = content[start_pos:]
            else:
                next_match = dsjob_matches[dsjob_matches.index(match) + 1]
                target_job_content = content[start_pos:next_match.start()]
            break
    
    if not target_job_content:
        print("m_DM_CUST_a01 Job을 찾을 수 없습니다.")
        return
    
    print(f"\n[Job 내용 길이: {len(target_job_content)} 문자]\n")
    
    # T_DM_CUST Stage 찾기
    print("[T_DM_CUST Stage 찾기]")
    stage_pattern = r'BEGIN DSRECORD\s+Identifier\s+"T_DM_CUST"(.*?)END DSRECORD'
    stage_match = re.search(stage_pattern, target_job_content, re.DOTALL)
    
    if stage_match:
        stage_content = stage_match.group(1)
        print(f"Stage 내용 길이: {len(stage_content)} 문자\n")
        
        # TableName 찾기
        table_name_match = re.search(r'TableName\s+"([^"]+)"', stage_content)
        schema_match = re.search(r'SchemaName\s+"([^"]+)"', stage_content)
        
        table_name = table_name_match.group(1) if table_name_match else None
        schema = schema_match.group(1) if schema_match else None
        
        print(f"TableName: {table_name}")
        print(f"SchemaName: {schema}\n")
        
        # XMLProperties 찾기
        xml_props_match = re.search(r'XMLProperties\s+Value\s+(?:=+=+=+=)?(.*?)(?:=+=+=+=)?\s+END DSSUBRECORD', stage_content, re.DOTALL)
        if xml_props_match:
            xml_props = xml_props_match.group(1).strip()
            print(f"XMLProperties 길이: {len(xml_props)} 문자")
            
            # Context 찾기
            context_match = re.search(r'<Context[^>]*>(.*?)</Context>', xml_props, re.DOTALL)
            if context_match:
                print(f"Context: {context_match.group(1).strip()}")
            else:
                print("Context: 없음")
            
            # TableName in XML
            xml_table_match = re.search(r'<TableName[^>]*><!\[CDATA\[(.*?)\]\]></TableName>', xml_props, re.DOTALL)
            if xml_table_match:
                print(f"XML TableName: {xml_table_match.group(1).strip()}")
            
            # SchemaName in XML
            xml_schema_match = re.search(r'<SchemaName[^>]*><!\[CDATA\[(.*?)\]\]></SchemaName>', xml_props, re.DOTALL)
            if xml_schema_match:
                print(f"XML SchemaName: {xml_schema_match.group(1).strip()}")
        
        # 컬럼 찾기
        print("\n[컬럼 정보 찾기]")
        column_pattern1 = r'Column\s+"([^"]+)"\s+Type\s+"([^"]+)"'
        columns1 = list(re.finditer(column_pattern1, stage_content))
        print(f"패턴1 (Column \"...\" Type \"...\"): {len(columns1)}개")
        for i, col in enumerate(columns1[:5], 1):
            print(f"  {i}. {col.group(1)} ({col.group(2)})")
        
        column_pattern2 = r'Column\s+"([^"]+)"'
        columns2 = list(re.finditer(column_pattern2, stage_content))
        print(f"\n패턴2 (Column \"...\"): {len(columns2)}개")
        for i, col in enumerate(columns2[:10], 1):
            print(f"  {i}. {col.group(1)}")
        
        # Stage의 일부 내용 출력
        print(f"\n[Stage 내용 샘플 (처음 500자)]")
        print(stage_content[:500])
        
    else:
        print("T_DM_CUST Stage를 찾을 수 없습니다.")
    
    # 파서로 분석
    print("\n" + "=" * 80)
    print("\n[파서로 분석한 결과]")
    parser = DSXParser()
    parsed_jobs = parser.parse_multiple_jobs(content, str(dsx_file))
    
    for job in parsed_jobs:
        if job.get("name") == "m_DM_CUST_a01":
            print(f"\nJob: {job.get('name')}")
            print(f"Source 테이블: {len(job.get('source_tables', []))}개")
            for table in job.get('source_tables', []):
                full_name = f"{table.get('schema', '')}.{table.get('table_name', '')}" if table.get('schema') else table.get('table_name', '')
                print(f"  - {full_name} (stage: {table.get('stage_name')}, type: {table.get('table_type')})")
            
            print(f"\nTarget 테이블: {len(job.get('target_tables', []))}개")
            for table in job.get('target_tables', []):
                full_name = f"{table.get('schema', '')}.{table.get('table_name', '')}" if table.get('schema') else table.get('table_name', '')
                print(f"  - {full_name} (stage: {table.get('stage_name')}, type: {table.get('table_type')})")
            
            # 컬럼 정보
            analyzer = DependencyAnalyzer()
            columns_data = analyzer._extract_columns(target_job_content)
            print(f"\n컬럼 정보: {len(columns_data)}개 테이블")
            for table_name, cols in columns_data.items():
                print(f"  - {table_name}: {len(cols)}개 컬럼")
                for col in cols[:3]:
                    print(f"    * {col.get('name')} ({col.get('type')})")
            break

if __name__ == "__main__":
    analyze_job_detail()

