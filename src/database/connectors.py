import pyodbc
import vertica_python
from typing import Dict, Any, List, Optional
from src.core.config import get_config
from src.core.logger import get_logger

logger = get_logger(__name__)

class DatabaseConnector:
    """데이터베이스 연결 관리 클래스"""
    
    def __init__(self):
        self.config = get_config()
    
    def get_mssql_connection(self):
        """MSSQL 연결 반환"""
        db_config = self.config.get_database_config("mssql")
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={db_config['server']},{db_config['port']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['username']};"
                f"PWD={db_config['password']}"
            )
            return pyodbc.connect(conn_str)
        except Exception as e:
            logger.error(f"MSSQL 연결 실패: {e}")
            return None

    def get_vertica_connection(self):
        """Vertica 연결 반환"""
        db_config = self.config.get_database_config("vertica")
        try:
            conn_info = {
                'host': db_config['host'],
                'port': db_config['port'],
                'user': db_config['username'],
                'password': db_config['password'],
                'database': db_config['database'],
                'autocommit': True
            }
            return vertica_python.connect(**conn_info)
        except Exception as e:
            logger.error(f"Vertica 연결 실패: {e}")
            return None

    def query_mssql(self, query: str) -> List[Dict[str, Any]]:
        """MSSQL 쿼리 실행"""
        conn = self.get_mssql_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            return results
        except Exception as e:
            logger.error(f"MSSQL 쿼리 실행 실패: {e}")
            return []
        finally:
            conn.close()

    def query_vertica(self, query: str) -> List[Dict[str, Any]]:
        """Vertica 쿼리 실행"""
        conn = self.get_vertica_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            return results
        except Exception as e:
            logger.error(f"Vertica 쿼리 실행 실패: {e}")
            return []
        finally:
            conn.close()

# 전역 인스턴스
_connector_instance = None

def get_connector():
    global _connector_instance
    if _connector_instance is None:
        _connector_instance = DatabaseConnector()
    return _connector_instance
