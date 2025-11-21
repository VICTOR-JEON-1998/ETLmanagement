from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.core.config import get_config
from src.core.logger import setup_logger, get_logger
from src.datastage.dependency_analyzer import DependencyAnalyzer
from src.datastage.erp_impact_analyzer import ERPImpactAnalyzer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ERP 컬럼 영향도 분석")
    parser.add_argument("--export-dir", required=True, help="DSX Export 디렉토리 경로")
    parser.add_argument("--column", required=True, help="변경할 컬럼명")
    parser.add_argument("--erp-table-file", required=True, help="ERP 테이블 목록 파일 (schema.table per line)")
    parser.add_argument("--max-level", type=int, default=2, help="연쇄 분석 최대 레벨 (기본: 2)")
    parser.add_argument("--output", help="JSON 결과 저장 경로")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = get_config()
    logging_config = config.get_logging_config()
    setup_logger(
        "etlmanagement",
        level=logging_config.get("level", "INFO"),
        log_format=logging_config.get("format"),
        log_file=logging_config.get("file"),
    )
    logger = get_logger(__name__)

    export_dir = Path(args.export_dir)
    if not export_dir.exists():
        raise FileNotFoundError(f"Export 디렉토리를 찾을 수 없습니다: {export_dir}")

    dependency_analyzer = DependencyAnalyzer(export_directory=str(export_dir), use_cache=True, resolve_parameters=True)
    erp_analyzer = ERPImpactAnalyzer(dependency_analyzer, export_directory=str(export_dir))
    erp_analyzer.load_erp_tables_from_file(args.erp_table_file)

    result = erp_analyzer.analyze_column(args.column, max_level=args.max_level)

    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"결과가 저장되었습니다: {output_path}")
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))

    logger.info("ERP 영향도 분석 완료")


if __name__ == "__main__":
    main()


