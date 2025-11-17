"""DDL 자동 생성 모듈"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from src.database.connectors import get_connector
from src.core.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ColumnChange:
    """컬럼 변경 정보"""
    column_name: str
    new_length: Optional[int] = None
    new_type: Optional[str] = None
    new_nullable: Optional[bool] = None
    new_name: Optional[str] = None


class DDLGenerator:
    """DDL 자동 생성 클래스"""
    
    def __init__(self, db_type: str = "mssql"):
        """
        DDL 생성자 초기화
        
        Args:
            db_type: 데이터베이스 타입 ("mssql" 또는 "vertica")
        """
        self.db_type = db_type.lower()
        self.db_connector = get_connector(db_type)
    
    def generate_column_change_ddl(
        self,
        table_name: str,
        schema: str,
        column_changes: List[ColumnChange]
    ) -> Dict[str, List[str]]:
        """
        3단계 DDL 자동 생성: PK 해제 → 컬럼 변경 → PK 재생성
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름
            column_changes: 컬럼 변경 정보 리스트
        
        Returns:
            DDL 스크립트 딕셔너리 (단계별)
        """
        logger.info(f"DDL 생성 시작: {schema}.{table_name}")
        
        # 현재 테이블 스키마 조회
        try:
            current_schema = self.db_connector.get_table_schema(table_name, schema)
        except Exception as e:
            logger.error(f"테이블 스키마 조회 실패: {e}")
            raise
        
        # PK 정보 추출
        pk_columns = [col for col in current_schema if col.get("is_pk")]
        pk_constraint_name = self._get_pk_constraint_name(table_name, schema)
        
        ddl_scripts = {
            "step1_drop_pk": [],
            "step2_alter_columns": [],
            "step3_recreate_pk": [],
            "pre_sql": []  # Pre-SQL로 실행할 전체 스크립트
        }
        
        # Step 1: PK 해제
        if pk_columns and pk_constraint_name:
            drop_pk_sql = self._generate_drop_pk_sql(
                table_name,
                schema,
                pk_constraint_name
            )
            ddl_scripts["step1_drop_pk"].append(drop_pk_sql)
        
        # Step 2: 컬럼 변경
        for change in column_changes:
            alter_sql = self._generate_alter_column_sql(
                table_name,
                schema,
                change,
                current_schema
            )
            if alter_sql:
                ddl_scripts["step2_alter_columns"].append(alter_sql)
        
        # Step 3: PK 재생성
        if pk_columns:
            create_pk_sql = self._generate_create_pk_sql(
                table_name,
                schema,
                pk_columns,
                pk_constraint_name
            )
            ddl_scripts["step3_recreate_pk"].append(create_pk_sql)
        
        # Pre-SQL 스크립트 생성 (전체 실행용)
        all_sql = []
        all_sql.extend(ddl_scripts["step1_drop_pk"])
        all_sql.extend(ddl_scripts["step2_alter_columns"])
        all_sql.extend(ddl_scripts["step3_recreate_pk"])
        ddl_scripts["pre_sql"] = all_sql
        
        logger.info(f"DDL 생성 완료: {len(all_sql)}개 SQL 문 생성")
        return ddl_scripts
    
    def _get_pk_constraint_name(self, table_name: str, schema: str) -> Optional[str]:
        """
        PK 제약 조건 이름 조회
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름
        
        Returns:
            PK 제약 조건 이름
        """
        if self.db_type == "mssql":
            query = """
            SELECT CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'PRIMARY KEY'
            """
            try:
                with self.db_connector.connect() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, (schema, table_name))
                    row = cursor.fetchone()
                    return row[0] if row else None
            except Exception as e:
                logger.warning(f"PK 제약 조건 이름 조회 실패: {e}")
                return f"PK_{table_name}"  # 기본 이름 반환
        
        elif self.db_type == "vertica":
            query = """
            SELECT constraint_name
            FROM v_catalog.table_constraints
            WHERE table_schema = :schema AND table_name = :table_name 
                AND constraint_type = 'PRIMARY KEY'
            """
            try:
                with self.db_connector.connect() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, {"schema": schema, "table_name": table_name})
                    row = cursor.fetchone()
                    return row[0] if row else None
            except Exception as e:
                logger.warning(f"PK 제약 조건 이름 조회 실패: {e}")
                return f"PK_{table_name}"  # 기본 이름 반환
        
        return None
    
    def _generate_drop_pk_sql(
        self,
        table_name: str,
        schema: str,
        constraint_name: str
    ) -> str:
        """PK 해제 SQL 생성"""
        if self.db_type == "mssql":
            return f"ALTER TABLE [{schema}].[{table_name}] DROP CONSTRAINT [{constraint_name}];"
        elif self.db_type == "vertica":
            return f"ALTER TABLE {schema}.{table_name} DROP CONSTRAINT {constraint_name};"
        else:
            raise ValueError(f"지원하지 않는 데이터베이스 타입: {self.db_type}")
    
    def _generate_alter_column_sql(
        self,
        table_name: str,
        schema: str,
        change: ColumnChange,
        current_schema: List[Dict[str, Any]]
    ) -> Optional[str]:
        """컬럼 변경 SQL 생성"""
        # 현재 컬럼 정보 찾기
        current_col = None
        for col in current_schema:
            if col["name"].lower() == change.column_name.lower():
                current_col = col
                break
        
        if not current_col:
            logger.warning(f"컬럼을 찾을 수 없습니다: {change.column_name}")
            return None
        
        # 컬럼 이름 변경
        if change.new_name:
            if self.db_type == "mssql":
                return f"EXEC sp_rename '{schema}.{table_name}.{change.column_name}', '{change.new_name}', 'COLUMN';"
            elif self.db_type == "vertica":
                return f"ALTER TABLE {schema}.{table_name} RENAME COLUMN {change.column_name} TO {change.new_name};"
        
        # 컬럼 타입/길이 변경
        alter_parts = []
        
        if change.new_type:
            col_type = change.new_type
            if change.new_length and col_type.upper() in ["VARCHAR", "CHAR", "NVARCHAR", "NCHAR"]:
                col_type = f"{col_type}({change.new_length})"
            alter_parts.append(col_type)
        
        if change.new_length and not change.new_type:
            # 타입은 그대로, 길이만 변경
            current_type = current_col["type"]
            if current_type.upper() in ["VARCHAR", "CHAR", "NVARCHAR", "NCHAR"]:
                alter_parts.append(f"{current_type}({change.new_length})")
        
        if change.new_nullable is not None:
            if change.new_nullable:
                alter_parts.append("NULL")
            else:
                alter_parts.append("NOT NULL")
        
        if not alter_parts:
            return None
        
        # SQL 생성
        column_name = change.new_name or change.column_name
        
        if self.db_type == "mssql":
            sql = f"ALTER TABLE [{schema}].[{table_name}] ALTER COLUMN [{column_name}] "
            sql += " ".join(alter_parts) + ";"
        elif self.db_type == "vertica":
            sql = f"ALTER TABLE {schema}.{table_name} ALTER COLUMN {column_name} "
            sql += " ".join(alter_parts) + ";"
        else:
            raise ValueError(f"지원하지 않는 데이터베이스 타입: {self.db_type}")
        
        return sql
    
    def _generate_create_pk_sql(
        self,
        table_name: str,
        schema: str,
        pk_columns: List[Dict[str, Any]],
        constraint_name: Optional[str]
    ) -> str:
        """PK 재생성 SQL 생성"""
        if not constraint_name:
            constraint_name = f"PK_{table_name}"
        
        pk_col_names = [col["name"] for col in pk_columns]
        
        if self.db_type == "mssql":
            cols_str = ", ".join([f"[{col}]" for col in pk_col_names])
            return f"ALTER TABLE [{schema}].[{table_name}] ADD CONSTRAINT [{constraint_name}] PRIMARY KEY ({cols_str});"
        elif self.db_type == "vertica":
            cols_str = ", ".join(pk_col_names)
            return f"ALTER TABLE {schema}.{table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({cols_str});"
        else:
            raise ValueError(f"지원하지 않는 데이터베이스 타입: {self.db_type}")
    
    def save_ddl_to_file(
        self,
        ddl_scripts: Dict[str, List[str]],
        file_path: str,
        format: str = "sql"
    ) -> str:
        """
        DDL 스크립트를 파일로 저장
        
        Args:
            ddl_scripts: DDL 스크립트 딕셔너리
            file_path: 저장할 파일 경로
            format: 파일 형식 ("sql" 또는 "yaml")
        
        Returns:
            저장된 파일 경로
        """
        from pathlib import Path
        
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if format == "sql":
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write("-- Step 1: PK 해제\n")
                for sql in ddl_scripts.get("step1_drop_pk", []):
                    f.write(sql + "\n")
                
                f.write("\n-- Step 2: 컬럼 변경\n")
                for sql in ddl_scripts.get("step2_alter_columns", []):
                    f.write(sql + "\n")
                
                f.write("\n-- Step 3: PK 재생성\n")
                for sql in ddl_scripts.get("step3_recreate_pk", []):
                    f.write(sql + "\n")
                
                f.write("\n-- Pre-SQL 전체 스크립트\n")
                for sql in ddl_scripts.get("pre_sql", []):
                    f.write(sql + "\n")
        
        elif format == "yaml":
            import yaml
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(ddl_scripts, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"DDL 스크립트 저장 완료: {file_path}")
        return str(file_path)

