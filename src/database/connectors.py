"""데이터베이스 연결 모듈"""

import pyodbc
import vertica_python
from typing import Dict, Any, Optional, List, Tuple
from contextlib import contextmanager

from src.core.config import get_config
from src.core.logger import get_logger

logger = get_logger(__name__)


class MSSQLConnector:
    """MSSQL 데이터베이스 연결 클래스"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        MSSQL 연결 초기화
        
        Args:
            config: MSSQL 설정 딕셔너리 (None이면 config에서 로드)
        """
        if config is None:
            config = get_config().get_database_config("mssql")
        
        self.server = config.get("server")
        self.port = config.get("port", 1433)
        self.database = config.get("database")
        self.username = config.get("username")
        self.password = config.get("password")
        
        self._connection_string = self._build_connection_string()
    
    def _build_connection_string(self) -> str:
        """연결 문자열 생성"""
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )
    
    @contextmanager
    def connect(self):
        """
        데이터베이스 연결 컨텍스트 매니저
        
        Yields:
            pyodbc.Connection: 데이터베이스 연결 객체
        """
        conn = None
        try:
            conn = pyodbc.connect(self._connection_string)
            logger.info(f"MSSQL 연결 성공: {self.server}:{self.port}/{self.database}")
            yield conn
        except Exception as e:
            logger.error(f"MSSQL 연결 실패: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("MSSQL 연결 종료")
    
    def get_table_schema(self, table_name: str, schema: str = "dbo") -> List[Dict[str, Any]]:
        """
        테이블 스키마 조회
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름 (기본값: dbo)
        
        Returns:
            컬럼 정보 리스트 (각 딕셔너리: name, type, length, nullable, is_pk)
        """
        query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.IS_NULLABLE,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PK
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
        ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA
            AND c.TABLE_NAME = pk.TABLE_NAME
            AND c.COLUMN_NAME = pk.COLUMN_NAME
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """
        
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (schema, table_name))
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row.COLUMN_NAME,
                    "type": row.DATA_TYPE,
                    "length": row.CHARACTER_MAXIMUM_LENGTH,
                    "nullable": row.IS_NULLABLE == "YES",
                    "is_pk": bool(row.IS_PK)
                })
            
            return columns
    
    def get_locks(self) -> List[Dict[str, Any]]:
        """
        현재 Lock 상태 조회
        
        Returns:
            Lock 정보 리스트
        """
        query = """
        SELECT 
            request_session_id,
            resource_database_id,
            resource_type,
            resource_associated_entity_id,
            request_mode,
            request_status
        FROM sys.dm_tran_locks
        WHERE resource_database_id = DB_ID()
        """
        
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            
            locks = []
            for row in cursor.fetchall():
                locks.append({
                    "session_id": row.request_session_id,
                    "database_id": row.resource_database_id,
                    "resource_type": row.resource_type,
                    "entity_id": row.resource_associated_entity_id,
                    "mode": row.request_mode,
                    "status": row.request_status
                })
            
            return locks
    
    def get_all_tables(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        모든 테이블 목록 조회
        
        Args:
            schema: 스키마 이름 (None이면 모든 스키마)
        
        Returns:
            테이블 정보 리스트 (각 딕셔너리: schema, table_name, table_type)
        """
        if schema:
            query = """
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
        else:
            query = """
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
        
        with self.connect() as conn:
            cursor = conn.cursor()
            if schema:
                cursor.execute(query, (schema,))
            else:
                cursor.execute(query)
            
            tables = []
            for row in cursor.fetchall():
                tables.append({
                    "schema": row.TABLE_SCHEMA,
                    "table_name": row.TABLE_NAME,
                    "table_type": row.TABLE_TYPE
                })
            
            return tables
    
    def get_schemas(self) -> List[str]:
        """
        모든 스키마 목록 조회
        
        Returns:
            스키마 이름 리스트
        """
        query = """
        SELECT DISTINCT SCHEMA_NAME
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('sys', 'information_schema', 'guest')
        ORDER BY SCHEMA_NAME
        """
        
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            
            schemas = []
            for row in cursor.fetchall():
                schemas.append(row.SCHEMA_NAME)
            
            return schemas


class VerticaConnector:
    """Vertica 데이터베이스 연결 클래스"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Vertica 연결 초기화
        
        Args:
            config: Vertica 설정 딕셔너리 (None이면 config에서 로드)
        """
        if config is None:
            config = get_config().get_database_config("vertica")
        
        self.host = config.get("host")
        self.port = config.get("port", 5433)
        self.database = config.get("database")
        self.username = config.get("username")
        self.password = config.get("password")
        
        self._conn_info = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.username,
            "password": self.password,
            "autocommit": True
        }
    
    @contextmanager
    def connect(self):
        """
        데이터베이스 연결 컨텍스트 매니저
        
        Yields:
            vertica_python.Connection: 데이터베이스 연결 객체
        """
        conn = None
        try:
            conn = vertica_python.connect(**self._conn_info)
            logger.info(f"Vertica 연결 성공: {self.host}:{self.port}/{self.database}")
            yield conn
        except Exception as e:
            logger.error(f"Vertica 연결 실패: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Vertica 연결 종료")
    
    def get_table_schema(self, table_name: str, schema: str = "public") -> List[Dict[str, Any]]:
        """
        테이블 스키마 조회
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름 (기본값: public)
        
        Returns:
            컬럼 정보 리스트 (각 딕셔너리: name, type, length, nullable, is_pk)
        """
        query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            CASE WHEN constraint_type = 'PRIMARY KEY' THEN 1 ELSE 0 END AS is_pk
        FROM (
            SELECT 
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.is_nullable,
                tc.constraint_type,
                ROW_NUMBER() OVER (PARTITION BY c.column_name ORDER BY tc.constraint_type DESC) AS rn
            FROM v_catalog.columns c
            LEFT JOIN v_catalog.constraint_columns cc ON c.table_schema = cc.table_schema
                AND c.table_name = cc.table_name
                AND c.column_name = cc.column_name
            LEFT JOIN v_catalog.table_constraints tc ON cc.constraint_name = tc.constraint_name
                AND cc.table_schema = tc.table_schema
            WHERE c.table_schema = :schema AND c.table_name = :table_name
        ) sub
        WHERE rn = 1
        ORDER BY ordinal_position
        """
        
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, {"schema": schema, "table_name": table_name})
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "length": row[2],
                    "nullable": row[3] == "YES",
                    "is_pk": bool(row[4])
                })
            
            return columns
    
    def get_locks(self) -> List[Dict[str, Any]]:
        """
        현재 Lock 상태 조회
        
        Returns:
            Lock 정보 리스트
        """
        query = """
        SELECT 
            transaction_id,
            object_name,
            lock_mode,
            lock_scope,
            request_timestamp
        FROM v_monitor.locks
        """
        
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            
            locks = []
            for row in cursor.fetchall():
                locks.append({
                    "transaction_id": row[0],
                    "object_name": row[1],
                    "lock_mode": row[2],
                    "lock_scope": row[3],
                    "request_timestamp": row[4]
                })
            
            return locks
    
    def get_all_tables(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        모든 테이블 목록 조회
        
        Args:
            schema: 스키마 이름 (None이면 모든 스키마)
        
        Returns:
            테이블 정보 리스트 (각 딕셔너리: schema, table_name, table_type)
        """
        # Vertica의 v_catalog.tables에는 table_type 컬럼이 없음
        # is_system_table 컬럼으로 시스템 테이블 제외
        if schema:
            query = """
            SELECT 
                table_schema,
                table_name,
                'TABLE' as table_type
            FROM v_catalog.tables
            WHERE table_schema = :schema 
                AND is_system_table = false
            ORDER BY table_schema, table_name
            """
        else:
            query = """
            SELECT 
                table_schema,
                table_name,
                'TABLE' as table_type
            FROM v_catalog.tables
            WHERE is_system_table = false
            ORDER BY table_schema, table_name
            """
        
        with self.connect() as conn:
            cursor = conn.cursor()
            if schema:
                cursor.execute(query, {"schema": schema})
            else:
                cursor.execute(query)
            
            tables = []
            for row in cursor.fetchall():
                tables.append({
                    "schema": row[0],
                    "table_name": row[1],
                    "table_type": row[2] if len(row) > 2 else "TABLE"
                })
            
            return tables
    
    def get_schemas(self) -> List[str]:
        """
        모든 스키마 목록 조회
        
        Returns:
            스키마 이름 리스트
        """
        query = """
        SELECT DISTINCT schema_name
        FROM v_catalog.schemata
        WHERE schema_name NOT IN ('v_catalog', 'v_monitor', 'v_internal')
        ORDER BY schema_name
        """
        
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            
            schemas = []
            for row in cursor.fetchall():
                schemas.append(row[0])
            
            return schemas


def get_connector(db_type: str, config: Optional[Dict[str, Any]] = None):
    """
    데이터베이스 커넥터 팩토리 함수
    
    Args:
        db_type: 데이터베이스 타입 ("mssql" 또는 "vertica")
        config: 설정 딕셔너리 (None이면 config에서 로드)
    
    Returns:
        데이터베이스 커넥터 인스턴스
    """
    if db_type.lower() == "mssql":
        return MSSQLConnector(config)
    elif db_type.lower() == "vertica":
        return VerticaConnector(config)
    else:
        raise ValueError(f"지원하지 않는 데이터베이스 타입: {db_type}")

