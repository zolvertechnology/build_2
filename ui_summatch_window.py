"""
SumMatch window — standalone combinatorial sum-matching UI.
Launched by run_SumMatch.py (APP_MODE = "SumMatch").

Shared infrastructure used:
  app_config       → product name / version / trial limit
  qt_compat        → PyQt5 / PyQt6 shim
  core_recon_engine → CandidateRuleEngine, LazyTableModel,
                       StandaloneSearchWorker
"""

import os
import sys
import ctypes
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import pandas as pd

import app_config
from app_config import get_profile, get_app_name

from qt_compat import (
    PYQT,
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QComboBox, QLineEdit, QTextEdit,
    QFileDialog, QMessageBox, QProgressBar, QFrame, QFont,
    QCheckBox, QTabWidget, QTableView, QDialog, QTextBrowser,
    QIcon, QTimer, QThread,
    _AlignCenter, _AlignRight,
    _Warning, _Ok, _RichText, _TextBrowser, _MultiSelection,
    exec_app, exec_dialog,
)

# QListWidget lives in QtWidgets — import directly to avoid changing qt_compat
try:
    from PyQt6.QtWidgets import QListWidget
except ImportError:
    from PyQt5.QtWidgets import QListWidget

from core_recon_engine import (
    LazyTableModel, StandaloneSearchWorker,
)

# ── Module-level sentinel ──────────────────────────────────────────────────────
_NO_NUMERIC_COLUMNS_SENTINEL = "__NO_NUMERIC_COLUMNS__"


# ── Help text factory ──────────────────────────────────────────────────────────
def _build_help(app_name: str) -> dict:
    return {
        "load_file": f"""\
<h3>{app_name} &mdash; Load Data</h3>
<p>Click <b>"Load CSV/XLSX File"</b> to import your dataset.</p>
<p><b>Supported formats:</b> CSV (.csv) and Excel (.xlsx, .xls).</p>
<p>After loading, your columns appear in the <em>Available Columns</em>
list and the Amount Column dropdown. Use the <em>Dataset Preview</em>
tab to inspect your data.</p>
""",
        "candidate_columns": f"""\
<h3>{app_name} &mdash; Grouping Columns</h3>
<p>{app_name} searches for rows that sum to your target by using the
categories in your grouping columns. <b>These columns define the search
space.</b></p>
<p><b>Example:</b> If your file contains <em>Vendor</em>,
<em>Department</em>, and <em>Amount</em>, selecting <em>Vendor</em>
and <em>Department</em> tells {app_name} to look for combinations of
vendor+department groups whose amounts sum to the target.</p>
<p><b>Trade-off:</b></p>
<ul>
  <li><b>Too few</b> — may miss solutions.</li>
  <li><b>Too many</b> — search may become slow.</li>
</ul>
<p><b>Good rule of thumb:</b> start with 2–4 meaningful columns.
Use <b>Add &gt;&gt;</b> / <b>&lt;&lt; Remove</b> to manage selection.</p>
""",
        "amount_column": f"""\
<h3>{app_name} &mdash; Amount Column</h3>
<p>Select the numeric column whose values will be summed and compared
against your target.</p>
<p>Only <b>purely numeric</b> columns are listed. If your desired
column is missing, it likely contains text, dates, or blank values —
clean the source file and reload.</p>
<p>Rows with a zero amount are automatically excluded because they
cannot change any sum.</p>
""",
        "target_amount": f"""\
<h3>{app_name} &mdash; Target Amount</h3>
<p>Enter the total you want matching rows to sum to.</p>
<p><b>Examples:</b> &nbsp; 1000 &nbsp;|&nbsp; 1000.5 &nbsp;|&nbsp;
-500.25</p>
<p>Negative targets are fully supported.</p>
""",
        "tolerance": f"""\
<h3>{app_name} &mdash; Error Tolerance</h3>
<p>The maximum acceptable difference between a matched sum and your
target. Default is <b>0.1</b>.</p>
<table cellpadding="4">
  <tr><td>Target = 1000, Tol = 0.1</td>
      <td>&rarr; accepts 999.9 – 1000.1</td></tr>
  <tr><td>Target = 1000, Tol = 0</td>
      <td>&rarr; exact match only</td></tr>
  <tr><td>Target = 1000, Tol = 1</td>
      <td>&rarr; accepts 999 – 1001</td></tr>
</table>
<p>A small tolerance (0.01–0.1) handles rounding in financial data.</p>
""",
        "start_solving": f"""\
<h3>{app_name} &mdash; Start Solving</h3>
<p>Click <b>"Start Solving"</b> to search for row subsets that sum to
your target. Progress and solutions appear in the Output Log.</p>
<p><b>Before starting, ensure:</b></p>
<ol>
  <li>A file is loaded.</li>
  <li>At least one grouping column is selected.</li>
  <li>An amount column is chosen.</li>
  <li>A target amount is entered.</li>
</ol>
""",
        "stop": f"""\
<h3>{app_name} &mdash; Stop</h3>
<p>Click <b>"Stop Running"</b> to cancel the search.</p>
<p>The search halts as soon as possible. Any solutions already found
are preserved — you can still view and export them.</p>
""",
        "export": f"""\
<h3>{app_name} &mdash; Export to Excel</h3>
<p>Saves your original data plus a <b>solution_set</b> column
indicating which solution(s) each row belongs to.</p>
<table cellpadding="4">
  <tr><td><code>1</code></td>
      <td>Row is in Solution 1 only</td></tr>
  <tr><td><code>1_2</code></td>
      <td>Row is in both Solution 1 and Solution 2</td></tr>
  <tr><td><em>(empty)</em></td>
      <td>Row is not part of any solution</td></tr>
</table>
""",
        "results": f"""\
<h3>{app_name} &mdash; Understanding Results</h3>
<p>The <em>Results</em> tab shows your data with an added
<b>solution_set</b> column.</p>
<table cellpadding="4">
  <tr><td><code>1</code></td>
      <td>Row belongs to Solution 1 only</td></tr>
  <tr><td><code>1_2</code></td>
      <td>Row belongs to Solutions 1 and 2</td></tr>
  <tr><td><em>(empty)</em></td>
      <td>Row is not part of any solution</td></tr>
</table>
<p>Use <b>"Export XLSX"</b> to save.</p>
""",
        "output_log": f"""\
<h3>{app_name} &mdash; Output Log</h3>
<p>Shows progress messages, each solution as it is found (with its
sum), elapsed time, and any errors or warnings.</p>
""",
    }


# ══════════════════════════════════════════════════════════════════════════════
#  SumMatchWindow
# ══════════════════════════════════════════════════════════════════════════════

class SumMatchWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self._profile  = get_profile()
        self._app_name = get_app_name()
        self._help     = _build_help(self._app_name)

        self.setWindowTitle(
            f"{self._app_name} v{self._profile['version']}")
        self.resize(1200, 800)

        # Data state
        self.df:                 Optional[pd.DataFrame] = None
        self.original_df:        Optional[pd.DataFrame] = None
        self.candidate_df:       Optional[pd.DataFrame] = None
        self.originalColumns:    List[str] = []
        self.selectedColumns:    List[str] = []
        self.numericColumns:     set = set()
        self.loaded_file_basename: Optional[str] = None

        # Thread management
        self.thread: Optional[QThread] = None
        self.worker: Optional[StandaloneSearchWorker] = None

        # Elapsed timer
        self.timer = QTimer()
        self.timer.timeout.connect(self._update_elapsed)
        self.searchStartTime: Optional[float] = None

        # Window icon (looks for {AppName}.png next to the script)
        try:
            icon_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self._app_name + ".png")
            if os.path.exists(icon_path):
                self.setWindowIcon(QIcon(icon_path))
        except Exception:
            pass

        self._init_ui()
        self._set_guide_step(1)

    # ══════════════════════════════════════════════════════════════════════
    #  Help system
    # ══════════════════════════════════════════════════════════════════════

    def _help_btn(self, key: str, title: str = ""):
        html = self._help.get(key, "")
        btn  = QPushButton(" ? ")
        btn.setFixedSize(24, 24)
        btn.setToolTip("Click for help")
        btn.setStyleSheet(
            "QPushButton{font-weight:bold;font-size:12px;"
            "border-radius:12px;background:#4a86c8;color:white;"
            "border:none;padding:0}"
            "QPushButton:hover{background:#3a76b8}"
            "QPushButton:pressed{background:#2a66a8}")
        t = title or f"{self._app_name} — Help"
        btn.clicked.connect(
            lambda _c=False, h=html, dt=t: self._show_help(h, dt))
        return btn

    def _show_help(self, html: str, title: str):
        d = QDialog(self)
        d.setWindowTitle(title)
        d.setMinimumSize(400, 260)
        d.resize(520, 400)
        lo = QVBoxLayout(d)
        br = QTextBrowser()
        br.setReadOnly(True)
        br.setOpenExternalLinks(True)
        br.setHtml(
            f"<div style='font-family:Segoe UI,Arial;font-size:10pt;"
            f"line-height:1.5;padding:8px'>{html}</div>")
        lo.addWidget(br)
        cb = QPushButton("Close")
        cb.clicked.connect(d.accept)
        cb.setDefault(True)
        lo.addWidget(cb, alignment=_AlignRight)
        exec_dialog(d)

    # ══════════════════════════════════════════════════════════════════════
    #  Step guide
    # ══════════════════════════════════════════════════════════════════════

    def _create_step_frame(self, title_text: str, desc_text: str):
        frame = QFrame()
        frame.setObjectName("stepFrame")
        frame.setStyleSheet(
            "#stepFrame{background:#F5F5F5;"
            "border:1px solid #D0D0D0;border-radius:8px;}")
        lay = QVBoxLayout(frame)
        lay.setContentsMargins(15, 10, 15, 10)
        title = QLabel(title_text)
        title.setStyleSheet(
            "font-weight:bold;font-size:13px;color:#707070;")
        desc = QLabel(desc_text)
        desc.setStyleSheet("font-size:11px;color:#909090;")
        desc.setWordWrap(True)
        lay.addWidget(title)
        lay.addWidget(desc)
        return frame, title, desc

    def _set_guide_step(self, step: int):
        BLUE_F  = ("#stepFrame{background:#E3F2FD;"
                   "border:2px solid #2196F3;border-radius:8px;}")
        GREEN_F = ("#stepFrame{background:#E8F5E9;"
                   "border:2px solid #66BB6A;border-radius:8px;}")
        GREY_F  = ("#stepFrame{background:#F5F5F5;"
                   "border:1px solid #D0D0D0;border-radius:8px;}")

        T_BLUE  = ("font-weight:bold;font-size:13px;color:#1565C0;"
                   "background:transparent;")
        T_GREEN = ("font-weight:bold;font-size:13px;color:#2E7D32;"
                   "background:transparent;")
        T_GREY  = ("font-weight:bold;font-size:13px;color:#707070;"
                   "background:transparent;")

        D_BLUE  = "font-size:11px;color:#1E88E5;background:transparent;"
        D_GREEN = "font-size:11px;color:#558B2F;background:transparent;"
        D_GREY  = "font-size:11px;color:#909090;background:transparent;"

        SOLVE_DESC = (
            "Select grouping columns, amount column, target amount, "
            "tolerance and click 'Start Solving'.")

        if step == 1:
            self.step1_frame.setStyleSheet(BLUE_F)
            self.step1_title.setText("Step 1: Load Data")
            self.step1_title.setStyleSheet(T_BLUE)
            self.step1_desc.setText("Import your CSV or Excel file.")
            self.step1_desc.setStyleSheet(D_BLUE)
            self.step2_frame.setStyleSheet(GREY_F)
            self.step2_title.setText("Step 2: Configure & Solve")
            self.step2_title.setStyleSheet(T_GREY)
            self.step2_desc.setText(SOLVE_DESC)
            self.step2_desc.setStyleSheet(D_GREY)
            self.guide_arrow.setStyleSheet(
                "font-size:20px;color:#B0B0B0;")

        elif step == 2:
            self.step1_frame.setStyleSheet(GREEN_F)
            self.step1_title.setText("✅  Step 1: Data Loaded!")
            self.step1_title.setStyleSheet(T_GREEN)
            self.step1_desc.setText("File ready.")
            self.step1_desc.setStyleSheet(D_GREEN)
            self.step2_frame.setStyleSheet(BLUE_F)
            self.step2_title.setText("Step 2: Configure & Solve")
            self.step2_title.setStyleSheet(T_BLUE)
            self.step2_desc.setText(SOLVE_DESC)
            self.step2_desc.setStyleSheet(D_BLUE)
            self.guide_arrow.setStyleSheet(
                "font-size:20px;color:#2196F3;")

        elif step == 3:
            self.step1_frame.setStyleSheet(GREEN_F)
            self.step1_title.setText("✅  Step 1: Data Loaded!")
            self.step1_title.setStyleSheet(T_GREEN)
            self.step1_desc.setText("File ready.")
            self.step1_desc.setStyleSheet(D_GREEN)
            self.step2_frame.setStyleSheet(GREEN_F)
            self.step2_title.setText("✅  Step 2: Analysis Complete!")
            self.step2_title.setStyleSheet(T_GREEN)
            self.step2_desc.setText(
                "Results ready! You can now export as .xlsx.")
            self.step2_desc.setStyleSheet(D_GREEN)
            self.guide_arrow.setStyleSheet(
                "font-size:20px;color:#66BB6A;")

        elif step == 4:   # solving in progress
            self.step1_frame.setStyleSheet(GREEN_F)
            self.step1_title.setText("✅  Step 1: Data Loaded!")
            self.step1_title.setStyleSheet(T_GREEN)
            self.step1_desc.setText("File ready.")
            self.step1_desc.setStyleSheet(D_GREEN)
            self.step2_frame.setStyleSheet(BLUE_F)
            self.step2_title.setText("Step 2: Solving…")
            self.step2_title.setStyleSheet(T_BLUE)
            self.step2_desc.setText("")
            self.step2_desc.setStyleSheet(D_BLUE)
            self.guide_arrow.setStyleSheet(
                "font-size:20px;color:#2196F3;")

    # ══════════════════════════════════════════════════════════════════════
    #  UI construction
    # ══════════════════════════════════════════════════════════════════════

    def _init_ui(self):
        central  = QWidget()
        self.setCentralWidget(central)
        main_lay = QVBoxLayout(central)

        # ── Step guide ─────────────────────────────────────────────────
        guide_w = QWidget()
        guide_w.setStyleSheet(
            "background:white;border-bottom:1px solid #E0E0E0;")
        guide_lay = QHBoxLayout(guide_w)
        guide_lay.setContentsMargins(20, 15, 20, 15)
        guide_lay.setSpacing(15)

        self.step1_frame, self.step1_title, self.step1_desc = \
            self._create_step_frame(
                "Step 1: Load Data",
                "Import your CSV or Excel file.")
        self.guide_arrow = QLabel("➜")
        self.guide_arrow.setStyleSheet(
            "font-size:20px;color:#B0B0B0;")
        self.guide_arrow.setAlignment(_AlignCenter)
        self.step2_frame, self.step2_title, self.step2_desc = \
            self._create_step_frame(
                "Step 2: Configure & Solve",
                "Select grouping columns, amount column, "
                "target amount, tolerance and click 'Start Solving'.")

        guide_lay.addWidget(self.step1_frame, 1)
        guide_lay.addWidget(self.guide_arrow)
        guide_lay.addWidget(self.step2_frame, 1)
        main_lay.addWidget(guide_w)

        # ── Load file row ───────────────────────────────────────────────
        top_lay = QHBoxLayout()
        self.btn_load = QPushButton("Load CSV/XLSX File")
        self.btn_load.setToolTip("Import a CSV or Excel file to begin")
        self.btn_load.clicked.connect(self._load_file)
        top_lay.addWidget(self.btn_load)
        top_lay.addWidget(
            self._help_btn("load_file",
                           f"{self._app_name} — Load Data"))
        top_lay.addStretch()
        main_lay.addLayout(top_lay)

        # ── Dual-list column selector ───────────────────────────────────
        col_lay = QHBoxLayout()

        # Available Columns (left pane)
        avail_v = QVBoxLayout()
        avail_h = QHBoxLayout()
        avail_h.addWidget(QLabel("Available Columns:"))
        avail_h.addWidget(
            self._help_btn("candidate_columns",
                           f"{self._app_name} — Grouping Columns"))
        avail_h.addStretch()
        avail_v.addLayout(avail_h)
        self.available_list = QListWidget()
        self.available_list.setSelectionMode(_MultiSelection)
        self.available_list.setToolTip(
            "Columns available for grouping — "
            "select and click Add >>")
        avail_v.addWidget(self.available_list)
        col_lay.addLayout(avail_v)

        # Middle transfer buttons
        mid_v = QVBoxLayout()
        mid_v.addStretch()
        for label, slot in (
                ("Add >>",          self._add_columns),
                ("<< Remove",       self._remove_columns),
                ("Add All >>",      self._add_all_columns),
                ("<< Remove All",   self._remove_all_columns),
        ):
            b = QPushButton(label)
            b.clicked.connect(slot)
            mid_v.addWidget(b)
        mid_v.addStretch()
        col_lay.addLayout(mid_v)

        # Selected Grouping Columns (right pane)
        sel_v = QVBoxLayout()
        sel_h = QHBoxLayout()
        sel_h.addWidget(QLabel("Selected Grouping Columns:"))
        sel_h.addWidget(
            self._help_btn("candidate_columns",
                           f"{self._app_name} — Grouping Columns"))
        sel_h.addStretch()
        sel_v.addLayout(sel_h)
        self.selected_list = QListWidget()
        self.selected_list.setSelectionMode(_MultiSelection)
        self.selected_list.setToolTip(
            "Columns used to describe matching row groups")
        sel_v.addWidget(self.selected_list)
        col_lay.addLayout(sel_v)

        main_lay.addLayout(col_lay)

        # ── Sort checkboxes ─────────────────────────────────────────────
        sort_lay = QHBoxLayout()
        self.chk_sort_avail = QCheckBox(
            "Sort Available Columns Alphabetically")
        self.chk_sort_avail.setChecked(False)
        self.chk_sort_avail.stateChanged.connect(
            self._refresh_column_lists)
        self.chk_sort_sel = QCheckBox(
            "Sort Selected Columns Alphabetically")
        self.chk_sort_sel.setChecked(False)
        self.chk_sort_sel.stateChanged.connect(
            self._refresh_column_lists)
        sort_lay.addWidget(self.chk_sort_avail)
        sort_lay.addStretch()
        sort_lay.addWidget(self.chk_sort_sel)
        main_lay.addLayout(sort_lay)

        # ── Config row ──────────────────────────────────────────────────
        cfg_lay = QHBoxLayout()

        cfg_lay.addWidget(QLabel("Amount Column:"))
        self.combo_amount = QComboBox()
        self.combo_amount.setMinimumWidth(300)
        self.combo_amount.setToolTip(
            "The numeric column to sum (only purely numeric columns "
            "are listed)")
        cfg_lay.addWidget(self.combo_amount)
        cfg_lay.addWidget(
            self._help_btn("amount_column",
                           f"{self._app_name} — Amount Column"))

        cfg_lay.addWidget(QLabel("Target Amount:"))
        self.edit_target = QLineEdit()
        self.edit_target.setPlaceholderText("e.g. 1000.25")
        self.edit_target.setToolTip(
            "The total the matching rows must sum to")
        cfg_lay.addWidget(self.edit_target)
        cfg_lay.addWidget(
            self._help_btn("target_amount",
                           f"{self._app_name} — Target Amount"))

        cfg_lay.addWidget(QLabel("Tolerance:"))
        self.edit_tol = QLineEdit("0.1")
        self.edit_tol.setPlaceholderText("0.1")
        self.edit_tol.setMaximumWidth(80)
        self.edit_tol.setToolTip(
            "Max difference from target (0 = exact match)")
        cfg_lay.addWidget(self.edit_tol)
        cfg_lay.addWidget(
            self._help_btn("tolerance",
                           f"{self._app_name} — Tolerance"))

        # Optional developer controls (gated by profile flags)
        p = self._profile
        if p.get("show_static_ordering", False):
            self.chk_static = QCheckBox(
                "Static Column Ordering (Low→High)")
            cfg_lay.addWidget(self.chk_static)
        else:
            self.chk_static = None

        if p.get("show_subset_generation_mode", False):
            cfg_lay.addWidget(QLabel("Subset Mode:"))
            self.combo_subset_mode = QComboBox()
            self.combo_subset_mode.addItems(
                ["Original Mode", "New Mode"])
            cfg_lay.addWidget(self.combo_subset_mode)
        else:
            self.combo_subset_mode = None

        main_lay.addLayout(cfg_lay)

        # ── Action buttons ──────────────────────────────────────────────
        btn_lay = QHBoxLayout()

        self.btn_solve = QPushButton("Start Solving")
        self.btn_solve.setToolTip(
            "Begin searching for rows that sum to the target")
        self.btn_solve.setStyleSheet(
            "QPushButton{background:#4CAF50;color:white;"
            "font-weight:bold;padding:6px 14px}"
            "QPushButton:hover{background:#45a049}"
            "QPushButton:disabled{background:#bdc3c7;color:#7f8c8d}")
        self.btn_solve.clicked.connect(self._start_solving)
        btn_lay.addWidget(self.btn_solve)
        btn_lay.addWidget(
            self._help_btn("start_solving",
                           f"{self._app_name} — Start Solving"))

        self.btn_stop = QPushButton("Stop Running")
        self.btn_stop.setToolTip(
            "Cancel the search (partial results are kept)")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self._stop_solving)
        btn_lay.addWidget(self.btn_stop)
        btn_lay.addWidget(
            self._help_btn("stop", f"{self._app_name} — Stop"))

        self.btn_export = QPushButton("Export XLSX")
        self.btn_export.setToolTip("Save results to an Excel file")
        self.btn_export.setEnabled(False)
        self.btn_export.clicked.connect(self._export_xlsx)
        btn_lay.addWidget(self.btn_export)
        btn_lay.addWidget(
            self._help_btn("export",
                           f"{self._app_name} — Export"))

        btn_lay.addStretch()
        main_lay.addLayout(btn_lay)

        # ── Progress bar ────────────────────────────────────────────────
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setVisible(False)
        self.progress_bar.setFixedHeight(16)
        main_lay.addWidget(self.progress_bar)

        # ── Elapsed label ───────────────────────────────────────────────
        self.lbl_elapsed = QLabel("Elapsed Time: 0.0 sec")
        main_lay.addWidget(self.lbl_elapsed)

        # ── Tab widget ──────────────────────────────────────────────────
        self.tab_widget = QTabWidget()

        preview_tab = QWidget()
        preview_lay = QVBoxLayout(preview_tab)
        self.preview_table = QTableView()
        preview_lay.addWidget(self.preview_table)
        self.tab_widget.addTab(preview_tab, "Dataset Preview")

        results_tab = QWidget()
        results_lay = QVBoxLayout(results_tab)
        res_h = QHBoxLayout()
        res_h.addWidget(QLabel("Results with solution_set column:"))
        res_h.addWidget(
            self._help_btn("results",
                           f"{self._app_name} — Results"))
        res_h.addStretch()
        results_lay.addLayout(res_h)
        self.result_table = QTableView()
        results_lay.addWidget(self.result_table)
        self.tab_widget.addTab(results_tab, "Results")

        main_lay.addWidget(self.tab_widget)

        # ── Output log ──────────────────────────────────────────────────
        log_h = QHBoxLayout()
        log_h.addWidget(QLabel("Output Log:"))
        log_h.addWidget(
            self._help_btn("output_log",
                           f"{self._app_name} — Output Log"))
        log_h.addStretch()
        main_lay.addLayout(log_h)

        self.output_text = QTextEdit()
        self.output_text.setReadOnly(True)
        self.output_text.setFont(QFont("Consolas", 9))
        main_lay.addWidget(self.output_text)

        # ── Trial info label (updated by main()) ───────────────────────
        self.lbl_trial = QLabel("")
        self.lbl_trial.setStyleSheet(
            "color:#c0392b;font-size:9pt;"
            "font-style:italic;padding:4px;")
        main_lay.addWidget(self.lbl_trial)

    # ══════════════════════════════════════════════════════════════════════
    #  Column list management
    # ══════════════════════════════════════════════════════════════════════

    def _refresh_column_lists(self):
        if not self.originalColumns:
            return
        available = [c for c in self.originalColumns
                     if c not in self.selectedColumns]
        if self.chk_sort_avail.isChecked():
            available = sorted(available)
        self.available_list.clear()
        for c in available:
            self.available_list.addItem(c)

        selected = list(self.selectedColumns)
        if self.chk_sort_sel.isChecked():
            selected = sorted(selected)
        else:
            selected = [c for c in self.originalColumns
                        if c in selected]
        self.selected_list.clear()
        for c in selected:
            self.selected_list.addItem(c)

    def _add_columns(self):
        for item in self.available_list.selectedItems():
            if item.text() not in self.selectedColumns:
                self.selectedColumns.append(item.text())
        self._refresh_column_lists()

    def _remove_columns(self):
        for item in self.selected_list.selectedItems():
            if item.text() in self.selectedColumns:
                self.selectedColumns.remove(item.text())
        self._refresh_column_lists()

    def _add_all_columns(self):
        self.selectedColumns = self.originalColumns[:]
        self._refresh_column_lists()

    def _remove_all_columns(self):
        self.selectedColumns = []
        self._refresh_column_lists()

    # ══════════════════════════════════════════════════════════════════════
    #  Table helpers
    # ══════════════════════════════════════════════════════════════════════

    def _update_table(self, table_view: QTableView,
                      df: pd.DataFrame):
        model = LazyTableModel(df)
        table_view.setModel(model)
        try:
            table_view.verticalScrollBar().valueChanged.disconnect()
        except (TypeError, RuntimeError):
            pass
        sb = table_view.verticalScrollBar()
        sb.valueChanged.connect(
            lambda v, m=model, s=sb:
                m.loadMore() if v >= s.maximum() else None)

    # ══════════════════════════════════════════════════════════════════════
    #  File loading
    # ══════════════════════════════════════════════════════════════════════

    def _detect_numeric_columns(self) -> set:
        result = set()
        if self.df is None:
            return result
        for col in self.df.columns:
            if pd.api.types.is_numeric_dtype(self.df[col]):
                result.add(col)
            else:
                try:
                    pd.to_numeric(self.df[col], errors="raise")
                    result.add(col)
                except (ValueError, TypeError):
                    pass
        return result

    def _load_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open CSV/XLSX File", "",
            "CSV files (*.csv);;Excel files (*.xlsx *.xls)")
        if not path:
            return
        try:
            p   = Path(path)
            ext = p.suffix.lower()
            df  = (pd.read_csv(path)
                   if ext == ".csv"
                   else pd.read_excel(path))

            # Free trial row cap
            limit = self._profile["free_trial_row_limit"]
            if app_config.FREE_TRIAL and df.shape[0] > limit:
                dlg = QMessageBox(self)
                dlg.setIcon(_Warning)
                dlg.setWindowTitle("Trial Limit Exceeded")
                dlg.setTextFormat(_RichText)
                dlg.setText(
                    f"<h3>Trial Limit Exceeded</h3>"
                    f"<p>Free trial allows up to <b>{limit}</b> rows.  "
                    f"This file has <b>{df.shape[0]:,}</b> rows.</p>"
                    f"<p>Purchase a licence at "
                    f"<a href='https://zolvertechnology.wixsite.com/home'>"
                    f"zolvertechnology.wixsite.com/home</a></p>")
                dlg.setTextInteractionFlags(_TextBrowser)
                exec_dialog(dlg)
                return

            self.df                   = df
            self.original_df          = df.copy()
            cols                      = list(df.columns)
            self.originalColumns      = cols[:]
            self.selectedColumns      = []
            self.loaded_file_basename = os.path.splitext(p.name)[0]

            self._refresh_column_lists()

            # Amount combo — numeric columns only
            self.numericColumns = self._detect_numeric_columns()
            self.combo_amount.clear()
            num_in_order = [c for c in cols
                            if c in self.numericColumns]
            if num_in_order:
                for c in num_in_order:
                    self.combo_amount.addItem(c, c)
            else:
                self.combo_amount.addItem(
                    "⚠ No purely numeric columns found",
                    _NO_NUMERIC_COLUMNS_SENTINEL)

            self._update_table(self.preview_table, self.original_df)
            self._set_guide_step(2)

            info = (f"File loaded: {len(df):,} rows "
                    f"× {len(cols)} columns.")
            if not num_in_order:
                info += (
                    "\n\n⚠ No purely numeric columns detected.\n"
                    "The Amount Column must contain only numeric "
                    "values — please check your data and reload.")
                QMessageBox.warning(
                    self,
                    "File Loaded — No Numeric Columns", info)
            else:
                QMessageBox.information(
                    self, "File Loaded", info)

        except Exception as e:
            QMessageBox.critical(
                self, "Error", f"Failed to load file: {e}")

    # ══════════════════════════════════════════════════════════════════════
    #  Solving
    # ══════════════════════════════════════════════════════════════════════

    def _cleanup_thread(self):
        """Stop and discard any previous search thread."""
        if self.thread is not None:
            if self.thread.isRunning():
                if self.worker is not None:
                    self.worker.cancel()
                self.thread.quit()
                if not self.thread.wait(5000):
                    self.thread.terminate()
                    self.thread.wait(2000)
            self.worker = None
            self.thread = None

    def _start_solving(self):
        # ── Validate inputs ────────────────────────────────────────────
        if self.df is None:
            QMessageBox.critical(
                self, "Error", "Please load a file first.")
            return

        candidate_cols = [
            self.selected_list.item(i).text()
            for i in range(self.selected_list.count())
        ]
        if not candidate_cols:
            QMessageBox.critical(
                self, "Error",
                "Please select at least one grouping column.")
            return

        amount_col = self.combo_amount.currentData()
        if not amount_col or amount_col == _NO_NUMERIC_COLUMNS_SENTINEL:
            QMessageBox.critical(
                self, "Error",
                "No valid Amount Column selected.\n\n"
                "Choose a purely numeric column "
                "(numbers only — no text, dates, or blanks).")
            return

        target_str = self.edit_target.text().strip()
        if not target_str:
            QMessageBox.critical(
                self, "Error", "Please enter a target amount.")
            return
        try:
            float(target_str)
        except ValueError:
            QMessageBox.critical(
                self, "Error",
                f"Target amount '{target_str}' is not a valid number.\n"
                "Please enter a numeric value (e.g. 1000.50).")
            return

        tol_str = self.edit_tol.text().strip() or "0.1"
        self.edit_tol.setText(tol_str)
        try:
            tol_val = float(tol_str)
            if tol_val < 0:
                QMessageBox.critical(
                    self, "Error",
                    "Tolerance cannot be negative.")
                return
        except ValueError:
            QMessageBox.critical(
                self, "Error",
                f"Tolerance '{tol_str}' is not a valid number.")
            return

        try:
            self.df[amount_col].astype(float)
        except (ValueError, TypeError):
            QMessageBox.critical(
                self, "Error",
                f"Amount column '{amount_col}' contains "
                f"non-numeric values.")
            return

        # ── Advanced options (gated by profile) ────────────────────────
        p = self._profile
        static_ordering = (
            self.chk_static.isChecked()
            if self.chk_static is not None
            else p.get("default_static_ordering", False))
        subset_mode = (
            self.combo_subset_mode.currentText()
            if self.combo_subset_mode is not None
            else p.get("default_subset_generation_mode",
                       "Original Mode"))

        # ── Prepare run ─────────────────────────────────────────────────
        self._update_table(self.result_table, pd.DataFrame())
        self._cleanup_thread()
        self._set_guide_step(4)

        self.btn_solve.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.btn_export.setEnabled(False)
        self.searchStartTime = time.time()
        self.timer.start(100)
        self.progress_bar.setVisible(True)

        self.output_text.clear()
        self.output_text.append("Start solving…\n")

        # ── Launch worker ───────────────────────────────────────────────
        self.thread = QThread()
        self.worker = StandaloneSearchWorker(
            df=self.df,
            candidate_cols=candidate_cols,
            amount_col=amount_col,
            target=target_str,
            tol_value=tol_str,
            static_ordering=static_ordering,
            subset_mode=subset_mode,
        )
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run_search)
        self.worker.finished.connect(self._on_search_finished)
        self.worker.progress.connect(self._append_output)
        self.worker.finished.connect(self.thread.quit)
        self.thread.start()

    def _stop_solving(self):
        if self.worker is not None:
            self.worker.cancel()
        self.output_text.append("Cancel requested by user.\n")

    def _on_search_finished(self, result_message: str,
                             candidate_df):
        self.timer.stop()
        self._update_elapsed()
        self.progress_bar.setVisible(False)
        self.output_text.append(result_message)
        self.btn_solve.setEnabled(True)
        self.btn_stop.setEnabled(False)

        if candidate_df is not None:
            self.candidate_df = candidate_df
            self._set_guide_step(3)
            self._update_table(self.result_table, candidate_df)
            self.tab_widget.setCurrentIndex(1)
            self.btn_export.setEnabled(True)
        else:
            self._set_guide_step(2)

    def _append_output(self, msg: str):
        self.output_text.append(msg)

    def _update_elapsed(self):
        if self.searchStartTime:
            e = time.time() - self.searchStartTime
            self.lbl_elapsed.setText(f"Elapsed Time: {e:.1f} sec")
        else:
            self.lbl_elapsed.setText("Elapsed Time: 0.0 sec")

    # ══════════════════════════════════════════════════════════════════════
    #  Export
    # ══════════════════════════════════════════════════════════════════════

    def _export_xlsx(self):
        if self.original_df is None:
            QMessageBox.warning(
                self, "Export Error", "No data available to export.")
            return

        save_dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix  = self._profile.get("export_suffix", "Results")
        base    = self.loaded_file_basename or self._app_name
        default = f"{base}_{suffix}_{save_dt}.xlsx"

        path, _ = QFileDialog.getSaveFileName(
            self, "Save Excel File", default,
            "Excel Files (*.xlsx)")
        if not path:
            return
        try:
            out = self.original_df.copy()
            if (self.candidate_df is not None
                    and "solution_set" in self.candidate_df.columns):
                out["solution_set"] = self.candidate_df["solution_set"]
            else:
                out["solution_set"] = ""
            out.to_excel(path, index=False)
            QMessageBox.information(
                self, "Export Successful",
                f"File saved successfully:\n{path}")
        except Exception as e:
            QMessageBox.critical(
                self, "Export Error",
                f"Failed to export Excel file: {e}")

    # ══════════════════════════════════════════════════════════════════════
    #  Close
    # ══════════════════════════════════════════════════════════════════════

    def closeEvent(self, event):
        self._cleanup_thread()
        event.accept()


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point — called by run_SumMatch.py
# ══════════════════════════════════════════════════════════════════════════════

def main():
    profile  = get_profile()
    app_name = get_app_name()
    version  = profile["version"]

    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
            f"{app_name}_{version}")
    except (ImportError, AttributeError, OSError):
        pass

    qt_app = QApplication(sys.argv)
    qt_app.setApplicationName(app_name)
    qt_app.setStyle("Fusion")

    try:
        icon_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            app_name + ".png")
        if os.path.exists(icon_path):
            qt_app.setWindowIcon(QIcon(icon_path))
    except Exception:
        pass

    # License check
    try:
        from license_protection import LicenseManager
        lm = LicenseManager(app_name)
        if not lm.load_license():
            if not lm.prompt_license():
                sys.exit(0)
        if lm.license_valid:
            app_config.FREE_TRIAL = False
    except ImportError:
        pass

    # Terms of service
    try:
        from terms_and_conditions import run_tos_check
        if not run_tos_check(app_name):
            sys.exit(0)
    except ImportError:
        pass

    window = SumMatchWindow()

    if app_config.FREE_TRIAL:
        limit = profile["free_trial_row_limit"]
        window.lbl_trial.setText(
            f"Free trial — {limit} row limit per file")
        window.output_text.append(
            f"Running in free trial mode "
            f"(limit: {limit} rows per file).")
    else:
        window.lbl_trial.setText("")
        window.output_text.append("Licensed — no row limits.")

    def _global_hook(exc_type, exc_value, exc_tb):
        tb_str = "".join(
            traceback.format_exception(exc_type, exc_value, exc_tb))
        try:
            window.output_text.append(
                f"[UNHANDLED ERROR]\n{tb_str}")
        except Exception:
            pass
        QMessageBox.critical(
            window, f"{app_name} — Unexpected Error",
            f"An unexpected error occurred:\n\n{exc_value}\n\n"
            f"Details are in the Output Log.")

    sys.excepthook = _global_hook
    window.show()
    sys.exit(exec_app(qt_app))