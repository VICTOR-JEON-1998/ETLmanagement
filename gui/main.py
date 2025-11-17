"""단일 DSX 분석 전용 GUI."""

from __future__ import annotations

import json
import re
import sys
import threading
from pathlib import Path
from typing import Dict

import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import get_config  # noqa: E402
from src.core.logger import get_logger, setup_logger  # noqa: E402
from src.datastage.dependency_analyzer import DependencyAnalyzer  # noqa: E402
from src.datastage.dsx_parser import DSXParser  # noqa: E402


config = get_config()
logging_config = config.get_logging_config()
setup_logger(
    "etlmanagement",
    level=logging_config.get("level", "INFO"),
    log_format=logging_config.get("format"),
    log_file=logging_config.get("file"),
)
logger = get_logger(__name__)


class DSXAnalyzerGUI:
    """DSX → Table → Column 분석만 제공하는 간단한 GUI."""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("DSX Analyzer")
        self.root.geometry("1100x760")

        self.parser = DSXParser()
        self.dependency_analyzer = DependencyAnalyzer(resolve_parameters=True)

        self._build_widgets()

    # ------------------------------------------------------------------ UI 구성
    def _build_widgets(self) -> None:
        header = ttk.Frame(self.root, padding=10)
        header.pack(fill=tk.X)

        ttk.Label(header, text="DSX 파일").pack(side=tk.LEFT)
        self.file_entry = ttk.Entry(header)
        self.file_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)

        ttk.Button(header, text="찾아보기", command=self._browse_file).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(header, text="분석 실행", command=self._start_analysis).pack(
            side=tk.LEFT
        )

        paned = ttk.Panedwindow(self.root, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        # 왼쪽: 결과 트리
        left = ttk.Frame(paned, padding=5)
        paned.add(left, weight=3)

        self.tree = ttk.Treeview(
            left,
            columns=("type", "info"),
            show="tree headings",
        )
        self.tree.heading("#0", text="이름")
        self.tree.heading("type", text="타입")
        self.tree.heading("info", text="정보")
        self.tree.column("#0", width=320)
        self.tree.pack(fill=tk.BOTH, expand=True)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

        tree_scroll = ttk.Scrollbar(left, orient=tk.VERTICAL, command=self.tree.yview)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.configure(yscrollcommand=tree_scroll.set)

        # 오른쪽: 로그 + 상세 정보
        right = ttk.Frame(paned, padding=5)
        paned.add(right, weight=2)

        log_frame = ttk.LabelFrame(right, text="분석 로그")
        log_frame.pack(fill=tk.BOTH, expand=True)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=12, wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)

        detail_frame = ttk.LabelFrame(right, text="선택 항목 정보")
        detail_frame.pack(fill=tk.BOTH, expand=True, pady=(6, 0))
        self.detail_text = scrolledtext.ScrolledText(
            detail_frame, height=12, wrap=tk.WORD
        )
        self.detail_text.pack(fill=tk.BOTH, expand=True)

        self.summary_var = tk.StringVar(value="DSX 파일을 선택한 뒤 분석을 실행하세요.")
        ttk.Label(self.root, textvariable=self.summary_var, padding=8).pack(fill=tk.X)

    # ------------------------------------------------------------------ 이벤트
    def _browse_file(self) -> None:
        filename = filedialog.askopenfilename(
            title="DSX 파일 선택",
            filetypes=[("DSX files", "*.dsx"), ("모든 파일", "*.*")],
            initialdir=self.file_entry.get() or None,
        )
        if filename:
            self.file_entry.delete(0, tk.END)
            self.file_entry.insert(0, filename)

    def _start_analysis(self) -> None:
        dsx_path = Path(self.file_entry.get().strip())
        if not dsx_path.exists():
            messagebox.showwarning("경고", "유효한 DSX 파일을 선택해주세요.")
            return

        self._reset_view()
        self._log(f"[INFO] 분석 시작: {dsx_path}")
        threading.Thread(
            target=self._analyze_file,
            args=(dsx_path,),
            daemon=True,
        ).start()

    def _reset_view(self) -> None:
        for child in self.tree.get_children():
            self.tree.delete(child)
        self.log_text.delete(1.0, tk.END)
        self.detail_text.delete(1.0, tk.END)
        self.summary_var.set("분석 중...")

    def _log(self, message: str) -> None:
        logger.info(message)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)

    # ------------------------------------------------------------------ 분석 로직
    def _analyze_file(self, dsx_path: Path) -> None:
        try:
            content = dsx_path.read_text(encoding="utf-8", errors="ignore")
            jobs = self.parser.parse_multiple_jobs(content, str(dsx_path))
            if not jobs:
                self._end_with_message("Job 정보를 찾을 수 없습니다.")
                return

            sections = self._split_jobs(content)
            total_tables = 0
            total_columns = 0

            for job in jobs:
                job_name = job.get("name", job.get("identifier", "Unknown"))
                job_node = self.tree.insert(
                    "",
                    tk.END,
                    text=job_name,
                    values=("Job", f"파일: {dsx_path.name}"),
                    tags=("job",),
                )

                job_content = sections.get(job_name, "")
                columns = self.dependency_analyzer._extract_columns(job_content)
                tables = self._collect_tables(job)

                total_tables += len(tables)
                for table_name, info in tables.items():
                    table_node = self.tree.insert(
                        job_node,
                        tk.END,
                        text=table_name,
                        values=("Table", info),
                        tags=("table",),
                    )

                    table_columns = columns.get(table_name, [])
                    if not table_columns:
                        self.tree.insert(
                            table_node,
                            tk.END,
                            text="(컬럼 정보 없음)",
                            values=("Column", ""),
                            tags=("column",),
                        )
                        continue

                    total_columns += len(table_columns)
                    for col in table_columns:
                        desc = f"Type: {col.get('type', 'Unknown')}"
                        if col.get("stage_name"):
                            desc += f" | Stage: {col['stage_name']}"
                        self.tree.insert(
                            table_node,
                            tk.END,
                            text=col.get("name", "Unknown"),
                            values=("Column", desc),
                            tags=("column",),
                        )

            self._end_with_message(
                f"총 {len(jobs)}개 Job / {total_tables}개 테이블 / {total_columns}개 컬럼 분석 완료"
            )
        except Exception as exc:  # pragma: no cover - 방어코드
            logger.exception("DSX 분석 실패")
            self._end_with_message(f"오류 발생: {exc}")

    def _end_with_message(self, message: str) -> None:
        self.summary_var.set(message)
        self._log(message)

    # ------------------------------------------------------------------ 데이터 처리 유틸
    def _split_jobs(self, content: str) -> Dict[str, str]:
        sections: Dict[str, str] = {}
        job_pattern = r"BEGIN DSJOB\s+(.*?)\s+END DSJOB"
        for match in re.finditer(job_pattern, content, re.DOTALL):
            block = match.group(0)
            name_match = re.search(r'Identifier\s+"([^"]+)"', block)
            if name_match:
                sections[name_match.group(1)] = block
        return sections

    def _collect_tables(self, job_info: dict) -> Dict[str, str]:
        tables: Dict[str, str] = {}
        for table in job_info.get("source_tables", []) + job_info.get("target_tables", []):
            schema = table.get("schema", "")
            name = table.get("table_name")
            if not name:
                continue
            full_name = f"{schema}.{name}" if schema else name
            if full_name in tables:
                continue

            stage_name = table.get("stage_name", "N/A")
            context = table.get("table_type", "unknown").title()
            info = f"{context} | Stage: {stage_name}"
            tables[full_name] = info
        return tables

    # ------------------------------------------------------------------ 트리 이벤트
    def _on_tree_select(self, _) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        item = selection[0]
        data = {
            "name": self.tree.item(item, "text"),
            "type": self.tree.item(item, "values")[0],
            "info": self.tree.item(item, "values")[1]
            if len(self.tree.item(item, "values")) > 1
            else "",
        }
        self.detail_text.delete(1.0, tk.END)
        self.detail_text.insert(tk.END, json.dumps(data, ensure_ascii=False, indent=2))


def main() -> None:
    root = tk.Tk()
    DSXAnalyzerGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
