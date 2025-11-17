"""DB 테이블 목록 조회 CLI 명령어"""

import click
import json
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connectors import get_connector
from src.core.logger import setup_logger, get_logger

config = get_config()
logging_config = config.get_logging_config()
setup_logger(
    "etlmanagement",
    level=logging_config.get("level", "INFO"),
    log_format=logging_config.get("format"),
    log_file=logging_config.get("file")
)
logger = get_logger(__name__)


@click.command()
@click.option("--db-type", required=True, type=click.Choice(["mssql", "vertica"]), help="데이터베이스 타입")
@click.option("--schema", help="스키마 이름 (선택, 없으면 모든 스키마)")
@click.option("--output", type=click.Path(), help="결과를 저장할 파일 경로 (JSON)")
def list_tables(db_type, schema, output):
    """DB별 모든 테이블 조회"""
    try:
        connector = get_connector(db_type)
        
        if schema:
            tables = connector.get_all_tables(schema)
        else:
            tables = connector.get_all_tables()
        
        # 스키마별로 그룹화
        tables_by_schema = {}
        for table in tables:
            schema_name = table["schema"]
            if schema_name not in tables_by_schema:
                tables_by_schema[schema_name] = []
            tables_by_schema[schema_name].append(table["table_name"])
        
        result = {
            "database_type": db_type,
            "schema_filter": schema,
            "total_tables": len(tables),
            "tables_by_schema": tables_by_schema,
            "all_tables": tables
        }
        
        if output:
            with open(output, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False, default=str)
            click.echo(f"결과가 저장되었습니다: {output}")
        else:
            click.echo(f"\n{db_type.upper()} 데이터베이스 테이블 목록")
            click.echo("=" * 60)
            click.echo(f"총 테이블 수: {len(tables)}\n")
            
            for schema_name, table_list in sorted(tables_by_schema.items()):
                click.echo(f"[{schema_name}] ({len(table_list)}개 테이블)")
                for table_name in sorted(table_list):
                    click.echo(f"  - {table_name}")
                click.echo()
    
    except Exception as e:
        logger.error(f"테이블 목록 조회 실패: {e}")
        click.echo(f"오류: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    from src.core.config import get_config
    list_tables()

