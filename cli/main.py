"""ETL Management System CLI"""

import click
import json
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import get_config
from src.core.logger import setup_logger, get_logger
from src.datastage.api_client import DataStageAPIClient
from src.datastage.metadata_manager import MetadataManager
from src.datastage.job_parser import JobParser
from src.database.connectors import get_connector
from src.database.schema_validator import SchemaValidator
from src.database.ddl_generator import DDLGenerator, ColumnChange
from src.monitoring.log_parser import LogParser
from src.monitoring.error_detector import ErrorDetector
from src.monitoring.lock_diagnostic import LockDiagnostic


# 로거 설정
config = get_config()
logging_config = config.get_logging_config()
setup_logger(
    "etlmanagement",
    level=logging_config.get("level", "INFO"),
    log_format=logging_config.get("format"),
    log_file=logging_config.get("file")
)
logger = get_logger(__name__)


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """ETL Management System - IBM DataStage 메타데이터 자동화 및 파이프라인 무결성 시스템"""
    pass


@cli.command()
@click.option("--project", required=True, help="프로젝트 이름")
@click.option("--table", required=True, help="테이블 이름")
@click.option("--schema", help="스키마 이름")
@click.option("--column", help="컬럼 이름 (선택)")
@click.option("--output", type=click.Path(), help="결과를 저장할 파일 경로 (JSON)")
def impact_analysis(project, table, schema, column, output):
    """영향도 분석: 특정 컬럼/테이블을 사용 중인 모든 Job 목록 조회"""
    try:
        manager = MetadataManager()
        result = manager.analyze_impact(project, table, schema, column)
        
        if output:
            with open(output, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False, default=str)
            click.echo(f"결과가 저장되었습니다: {output}")
        else:
            click.echo(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    
    except Exception as e:
        logger.error(f"영향도 분석 실패: {e}")
        click.echo(f"오류: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--project", required=True, help="프로젝트 이름")
@click.option("--table", required=True, help="테이블 이름")
@click.option("--schema", help="스키마 이름")
@click.option("--column", required=True, help="변경할 컬럼 이름")
@click.option("--new-name", help="새 컬럼 이름")
@click.option("--new-length", type=int, help="새 컬럼 길이")
@click.option("--new-type", help="새 컬럼 타입")
@click.option("--dry-run/--no-dry-run", default=True, help="실제 변경 없이 시뮬레이션만 수행")
def propagate_metadata(project, table, schema, column, new_name, new_length, new_type, dry_run):
    """메타데이터 일괄 전파: 컬럼 변경사항을 관련 Job에 자동 반영"""
    try:
        column_changes = {
            "column_name": column
        }
        if new_name:
            column_changes["new_name"] = new_name
        if new_length:
            column_changes["new_length"] = new_length
        if new_type:
            column_changes["new_type"] = new_type
        
        manager = MetadataManager()
        result = manager.propagate_metadata(project, table, schema, column_changes, dry_run)
        
        click.echo(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    
    except Exception as e:
        logger.error(f"메타데이터 전파 실패: {e}")
        click.echo(f"오류: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--job-def", type=click.Path(exists=True), help="Job 정의 파일 경로 (JSON)")
@click.option("--project", help="프로젝트 이름")
@click.option("--job-name", help="Job 이름")
@click.option("--db-type", default="mssql", type=click.Choice(["mssql", "vertica"]), help="데이터베이스 타입")
@click.option("--output", type=click.Path(), help="검증 리포트를 저장할 파일 경로 (JSON)")
def validate_schema(job_def, project, job_name, db_type, output):
    """스키마 무결성 검증: ETL Job과 DB 스키마 비교"""
    try:
        validator = SchemaValidator(db_type)
        
        # Job 정의 로드
        if job_def:
            import json
            with open(job_def, 'r', encoding='utf-8') as f:
                job_definition = json.load(f)
        elif project and job_name:
            api_client = DataStageAPIClient()
            job_definition = api_client.get_job_definition(project, job_name)
            if not job_definition:
                click.echo(f"Job을 찾을 수 없습니다: {project}/{job_name}", err=True)
                sys.exit(1)
        else:
            click.echo("--job-def 또는 --project/--job-name 옵션이 필요합니다", err=True)
            sys.exit(1)
        
        # 검증 수행
        issues = validator.validate_job_schema(job_definition, project)
        report = validator.generate_validation_report(issues)
        
        if output:
            with open(output, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            click.echo(f"검증 리포트가 저장되었습니다: {output}")
        else:
            click.echo(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    
    except Exception as e:
        logger.error(f"스키마 검증 실패: {e}")
        click.echo(f"오류: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--table", required=True, help="테이블 이름")
@click.option("--schema", default="dbo", help="스키마 이름")
@click.option("--column", required=True, help="변경할 컬럼 이름")
@click.option("--new-length", type=int, help="새 컬럼 길이")
@click.option("--new-type", help="새 컬럼 타입")
@click.option("--new-name", help="새 컬럼 이름")
@click.option("--db-type", default="mssql", type=click.Choice(["mssql", "vertica"]), help="데이터베이스 타입")
@click.option("--output", type=click.Path(), help="DDL 스크립트를 저장할 파일 경로")
def generate_ddl(table, schema, column, new_length, new_type, new_name, db_type, output):
    """DDL 자동 생성: PK 해제 → 컬럼 변경 → PK 재생성"""
    try:
        generator = DDLGenerator(db_type)
        
        # 컬럼 변경 정보 생성
        changes = [ColumnChange(
            column_name=column,
            new_length=new_length,
            new_type=new_type,
            new_name=new_name
        )]
        
        # DDL 생성
        ddl_scripts = generator.generate_column_change_ddl(table, schema, changes)
        
        # 출력
        if output:
            generator.save_ddl_to_file(ddl_scripts, output, format="sql")
            click.echo(f"DDL 스크립트가 저장되었습니다: {output}")
        else:
            click.echo("-- Step 1: PK 해제 --")
            for sql in ddl_scripts.get("step1_drop_pk", []):
                click.echo(sql)
            click.echo("\n-- Step 2: 컬럼 변경 --")
            for sql in ddl_scripts.get("step2_alter_columns", []):
                click.echo(sql)
            click.echo("\n-- Step 3: PK 재생성 --")
            for sql in ddl_scripts.get("step3_recreate_pk", []):
                click.echo(sql)
    
    except Exception as e:
        logger.error(f"DDL 생성 실패: {e}")
        click.echo(f"오류: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--log-file", type=click.Path(exists=True), help="로그 파일 경로")
@click.option("--log-dir", type=click.Path(exists=True), help="로그 디렉토리 경로")
@click.option("--pattern", default="*.log", help="로그 파일 패턴 (--log-dir 사용 시)")
@click.option("--output", type=click.Path(), help="분석 리포트를 저장할 파일 경로 (JSON)")
def parse_logs(log_file, log_dir, pattern, output):
    """로그 분석 및 오류 진단"""
    try:
        parser = LogParser()
        detector = ErrorDetector()
        
        # 로그 파싱
        if log_file:
            errors = parser.parse_log_file(log_file)
        elif log_dir:
            errors = parser.parse_log_directory(log_dir, pattern)
        else:
            click.echo("--log-file 또는 --log-dir 옵션이 필요합니다", err=True)
            sys.exit(1)
        
        if not errors:
            click.echo("오류가 발견되지 않았습니다")
            return
        
        # 오류 분석
        analyses = detector.analyze_errors(errors)
        report = detector.generate_error_report(errors, analyses)
        
        if output:
            with open(output, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            click.echo(f"분석 리포트가 저장되었습니다: {output}")
        else:
            click.echo(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    
    except Exception as e:
        logger.error(f"로그 분석 실패: {e}")
        click.echo(f"오류: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--db-type", default="mssql", type=click.Choice(["mssql", "vertica"]), help="데이터베이스 타입")
@click.option("--output", type=click.Path(), help="Lock 리포트를 저장할 파일 경로 (JSON)")
def diagnose_locks(db_type, output):
    """Lock 상태 진단"""
    try:
        diagnostic = LockDiagnostic(db_type)
        report = diagnostic.generate_lock_report()
        
        if output:
            with open(output, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            click.echo(f"Lock 리포트가 저장되었습니다: {output}")
        else:
            click.echo(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    
    except Exception as e:
        logger.error(f"Lock 진단 실패: {e}")
        click.echo(f"오류: {e}", err=True)
        sys.exit(1)


@cli.command()
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


@cli.command()
@click.option("--project", required=True, help="프로젝트 이름")
@click.option("--output", type=click.Path(), help="결과를 저장할 파일 경로 (JSON)")
def list_jobs(project, output):
    """ETL Job 목록 조회"""
    try:
        api_client = DataStageAPIClient()
        jobs = api_client.get_jobs(project)
        
        result = {
            "project": project,
            "total_jobs": len(jobs),
            "jobs": jobs
        }
        
        if output:
            with open(output, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False, default=str)
            click.echo(f"결과가 저장되었습니다: {output}")
        else:
            click.echo(f"\n프로젝트 '{project}'의 Job 목록")
            click.echo("=" * 60)
            click.echo(f"총 Job 수: {len(jobs)}\n")
            
            for job in jobs:
                click.echo(f"  - {job.get('name', 'Unknown')}")
                if job.get('file_path'):
                    click.echo(f"    파일: {job.get('file_path')}")
    
    except Exception as e:
        logger.error(f"Job 목록 조회 실패: {e}")
        click.echo(f"오류: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--project", required=True, help="프로젝트 이름")
@click.option("--job-name", required=True, help="Job 이름")
@click.option("--output", type=click.Path(), help="Job 정의를 저장할 파일 경로")
def get_job(project, job_name, output):
    """ETL Job 정의 불러오기"""
    try:
        api_client = DataStageAPIClient()
        job_def = api_client.get_job_definition(project, job_name)
        
        if not job_def:
            click.echo(f"Job을 찾을 수 없습니다: {project}/{job_name}", err=True)
            sys.exit(1)
        
        if output:
            if isinstance(job_def, dict) and "content" in job_def:
                # XML/DSX 파일로 저장
                with open(output, 'w', encoding='utf-8') as f:
                    f.write(job_def["content"])
                click.echo(f"Job 정의가 저장되었습니다: {output}")
            else:
                # JSON으로 저장
                with open(output, 'w', encoding='utf-8') as f:
                    json.dump(job_def, f, indent=2, ensure_ascii=False, default=str)
                click.echo(f"Job 정의가 저장되었습니다: {output}")
        else:
            if isinstance(job_def, dict) and "content" in job_def:
                click.echo(job_def["content"])
            else:
                click.echo(json.dumps(job_def, indent=2, ensure_ascii=False, default=str))
    
    except Exception as e:
        logger.error(f"Job 정의 조회 실패: {e}")
        click.echo(f"오류: {e}", err=True)
        sys.exit(1)


@cli.command()
def test_connection():
    """연결 테스트: DataStage 및 DB 연결 확인"""
    try:
        click.echo("DataStage 연결 테스트...")
        api_client = DataStageAPIClient()
        result = api_client.test_connection()
        if result["success"]:
            click.echo(f"✓ DataStage 연결 성공")
            method = result.get("method", "rest")
            if method == "ssh":
                click.echo(f"  연결 방법: SSH")
                ssh_info = result.get("ssh_info", {})
                if ssh_info.get("datastage_path"):
                    click.echo(f"  DataStage 경로: {ssh_info.get('datastage_path')}")
                if ssh_info.get("projects"):
                    click.echo(f"  프로젝트: {', '.join(ssh_info.get('projects', [])[:5])}")
                if ssh_info.get("version"):
                    click.echo(f"  버전: {ssh_info.get('version')}")
            else:
                click.echo(f"  연결 방법: REST API")
                click.echo(f"  엔드포인트: {result.get('endpoint', 'N/A')}")
                click.echo(f"  상태 코드: {result.get('status_code', 'N/A')}")
        else:
            click.echo("✗ DataStage 연결 실패")
            click.echo(f"  오류: {result.get('error', '알 수 없는 오류')}")
            click.echo("\n시도한 엔드포인트 (일부):")
            for detail in result.get("details", [])[:5]:  # 최대 5개만 표시
                status = detail.get("status_code", "N/A")
                error = detail.get("error", "")
                click.echo(f"  - {detail.get('url', 'N/A')} (Status: {status})")
                if error:
                    click.echo(f"    오류: {error}")
        
        click.echo("\nMSSQL 연결 테스트...")
        mssql_conn = get_connector("mssql")
        try:
            with mssql_conn.connect():
                click.echo("✓ MSSQL 연결 성공")
        except Exception as e:
            click.echo(f"✗ MSSQL 연결 실패: {e}")
        
        click.echo("\nVertica 연결 테스트...")
        vertica_conn = get_connector("vertica")
        try:
            with vertica_conn.connect():
                click.echo("✓ Vertica 연결 성공")
        except Exception as e:
            click.echo(f"✗ Vertica 연결 실패: {e}")
    
    except Exception as e:
        logger.error(f"연결 테스트 실패: {e}")
        click.echo(f"오류: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()

