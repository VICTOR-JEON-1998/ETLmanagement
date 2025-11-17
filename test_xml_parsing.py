"""XMLProperties 파싱 테스트"""

import xml.etree.ElementTree as ET
import re

# 실제 XMLProperties 내용
xml_str = """<?xml version='1.0' encoding='UTF-16'?><Properties version='1.1'><Common><Context type='int'>2</Context><Variant type='string'>3.5</Variant><DescriptorVersion type='string'>1.0</DescriptorVersion><PartitionType type='int'>-1</PartitionType><RCP type='int'>0</RCP></Common><Connection><DataSource modified='1' type='string'><![CDATA[#P_DW_VER.$P_DW_VER_ODBC#]]></DataSource><Username modified='1' type='string'><![CDATA[#P_DW_VER.$P_DW_VER_USER#]]></Username><Password modified='1' type='string'><![CDATA[#P_DW_VER.$P_DW_VER_PWD#]]></Password></Connection><Usage><WriteMode modified='1' type='int'><![CDATA[0]]></WriteMode><GenerateSQL modified='1' type='bool'><![CDATA[1]]></GenerateSQL><TableName modified='1' type='string'><![CDATA[#P_DW_VER.$P_DW_VER_OWN_BIDWADM#.DM_CUST]]></TableName><EnableQuotedIDs type='bool'><![CDATA[0]]></EnableQuotedIDs><SQL></SQL><TableAction modified='1' type='int'><![CDATA[3]]><GenerateTruncateStatement modified='1' type='bool'><![CDATA[0]]><FailOnError type='bool'><![CDATA[1]]></FailOnError><TruncateStatement modified='1' type='string'><![CDATA[TRUNCATE TABLE #P_DW_VER.$P_DW_VER_OWN_BIDWADM#.DM_CUST ;]]></TruncateStatement></GenerateTruncateStatement></TableAction><Transaction><RecordCount modified='1' type='int'><![CDATA[#P_ETL_SET.$P_ETL_RCNT#]]></RecordCount></Transaction><Session><IsolationLevel type='int'><![CDATA[1]]></IsolationLevel><AutocommitMode type='int'><![CDATA[0]]></AutocommitMode><ArraySize modified='1' type='int'><![CDATA[#P_ETL_SET.$P_ETL_ARSZ#]]></ArraySize><SchemaReconciliation><FailOnSizeMismatch type='bool'><![CDATA[1]]></FailOnSizeMismatch><FailOnTypeMismatch type='bool'><![CDATA[1]]></FailOnTypeMismatch><FailOnCodePageMismatch type='bool'><![CDATA[0]]></FailOnCodePageMismatch><DropUnmatchedFields type='bool'><![CDATA[1]]></DropUnmatchedFields></SchemaReconciliation><CodePage collapsed='1' type='int'><![CDATA[0]]></CodePage><FailOnRowErrorPX type='bool'><![CDATA[1]]></FailOnRowErrorPX></Session><Logging><LogColumnValues collapsed='1' type='bool'><![CDATA[0]]></LogColumnValues></Logging><BeforeAfter collapsed='1' modified='1' type='bool'><![CDATA[0]]></BeforeAfter></Usage></Properties >"""

print("XML 문자열 길이:", len(xml_str))
print("\n" + "=" * 80)
print("XML 파싱 테스트")
print("=" * 80)

try:
    root = ET.fromstring(xml_str)
    print("✓ XML 파싱 성공\n")
    
    # Context 찾기
    context_elems = root.findall(".//Context")
    print(f"Context 요소 개수: {len(context_elems)}")
    for i, ctx in enumerate(context_elems):
        print(f"  {i+1}. text={ctx.text}, attrib={ctx.attrib}")
        if ctx.text:
            print(f"     → 값: {ctx.text.strip()}")
    
    # TableName 찾기
    table_elems = root.findall(".//TableName")
    print(f"\nTableName 요소 개수: {len(table_elems)}")
    for i, tbl in enumerate(table_elems):
        print(f"  {i+1}. text={tbl.text}")
        if tbl.text:
            print(f"     → 값: {tbl.text.strip()}")
    
except Exception as e:
    print(f"✗ XML 파싱 실패: {e}")
    import traceback
    traceback.print_exc()

