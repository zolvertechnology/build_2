"""
Unified adaptive MainWindow — driven entirely by app_config.APP_MODE.
Supports: RecordLink, Dedupe, DedupeLink, AutoRecon.
"""

import sys
import os
import ctypes
import traceback
import tempfile
import json
import webbrowser
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd

import app_config
from app_config import get_profile, get_app_name

from qt_compat import (
    PYQT, HAS_WEBENGINE, HAS_MATPLOTLIB, FigureCanvas, Figure,
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGridLayout, QLabel, QPushButton, QComboBox, QLineEdit, QTextEdit,
    QFileDialog, QMessageBox, QTableWidget, QTableWidgetItem, QHeaderView,
    QTabWidget, QGroupBox, QSpinBox, QDoubleSpinBox, QCheckBox,
    QProgressBar, QSplitter, QFrame, QFont, QIcon, QDialog, QTextBrowser,
    QTableView, QTimer, QThread, QObject,
    _AlignCenter, _AlignRight, _Vertical, _Stretch, _NoEdit,
    _Warning, _Ok, _Yes, _No, _RichText, _TextBrowser,
    _MultiSelection, _ResetRole,
    exec_app, exec_dialog,
)

if HAS_WEBENGINE:
    from qt_compat import QWebEngineView  # type: ignore

from core_splink_engine import (
    SPLINK_VERSION, DEBUG, safe_slot,
    build_splink_settings, LinkWorker, chart_to_html, open_html_in_browser,
)
from core_recon_engine import (
    LazyTableModel, ReconWorker, StandaloneSearchWorker,
)

# ── Help texts ─────────────────────────────────────────────────────────────────

def _HELP_OVERVIEW(app_name):
    return f"""<h3>{app_name} — Quick-Start Guide</h3>
<ol>
  <li><b>Load</b> your dataset(s).</li>
  <li><b>Map Columns</b> — pair up columns and choose a comparison method.</li>
  <li><b>Run</b> — the engine trains a probabilistic model and scores pairs.</li>
  <li><b>Explore Results</b> — inspect matches and visualisations.</li>
  <li><b>Export</b> results to Excel or CSV.</li>
</ol>"""

HELP_COLUMN_MAPPING = """\
<h3>Column Mapping</h3>
<p>Map columns and choose a comparison type:</p>
<ul>
  <li><b>Exact</b> — strict equality.</li>
  <li><b>Levenshtein</b> — edit-distance fuzzy matching.</li>
  <li><b>Jaro-Winkler</b> — optimised for short strings / names.</li>
  <li><b>Date</b> — multi-level date comparison.</li>
</ul>
<p><b>Block?</b> — limits comparisons to pairs sharing the same value,
dramatically improving performance on large datasets.</p>"""

HELP_RECON_SETTINGS = """\
<h3>Reconciliation Settings</h3>
<p>After probabilistic matching identifies clusters of related records,
AutoRecon searches within each cluster for subsets whose amounts
<b>net to zero</b>.</p>
<ul>
  <li><b>Amount Column</b> — the numeric column containing transaction amounts.</li>
  <li><b>Recon Tolerance</b> — maximum acceptable deviation from zero
      (e.g. 0.01 for rounding differences).</li>
</ul>"""

FREE_TRIAL_ROW_LIMIT = 100

# ══════════════════════════════════════════════════════════════════════════════
#  MainWindow
# ══════════════════════════════════════════════════════════════════════════════

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self._profile   = get_profile()
        self._app_name  = get_app_name()
        self._mode      = app_config.APP_MODE

        self.setWindowTitle(
            f"{self._app_name} v{self._profile['version']}")
        self.setMinimumSize(1100, 750)

        self.df_left:     Optional[pd.DataFrame] = None
        self.df_right:    Optional[pd.DataFrame] = None
        self.df_results:  Optional[pd.DataFrame] = None
        self.linker       = None
        self.charts:      Dict[str, Any] = {}
        self.link_worker: Optional[LinkWorker] = None
        self.recon_worker: Optional[ReconWorker] = None

        # AutoRecon direct-recon (single-dataset path)
        self.recon_thread: Optional[QThread]  = None
        self.recon_solo_worker: Optional[StandaloneSearchWorker] = None

        # Timer for elapsed display
        self.timer        = QTimer()
        self.timer.timeout.connect(self._update_elapsed)
        self._start_time: Optional[float] = None

        self._build_ui()
        self._connect_signals()
        self._apply_profile_visibility()
        self._update_guidance()
        self.statusBar().showMessage(self._profile["status_ready"])

    # ══════════════════════════════════════════════════════════════════════
    #  Help system
    # ══════════════════════════════════════════════════════════════════════

    def _help_btn(self, html, title=None):
        btn = QPushButton(" ? ")
        btn.setFixedSize(24, 24)
        btn.setToolTip("Click for help")
        btn.setStyleSheet(
            "QPushButton{font-weight:bold;font-size:12px;border-radius:12px;"
            "background:#4a86c8;color:white;border:none;padding:0}"
            "QPushButton:hover{background:#3a76b8}"
            "QPushButton:pressed{background:#2a66a8}")
        t = title or f"{self._app_name} — Help"
        btn.clicked.connect(lambda _c=False, h=html, dt=t: self._show_help(h, dt))
        return btn

    def _show_help(self, html, title):
        d = QDialog(self)
        d.setWindowTitle(title)
        d.setMinimumSize(420, 280)
        d.resize(560, 440)
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
    #  UI construction
    # ══════════════════════════════════════════════════════════════════════

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        # Guidance banner
        guide_row = QHBoxLayout()
        self.guidance_label = QLabel()
        self.guidance_label.setWordWrap(True)
        self.guidance_label.setStyleSheet(
            "QLabel{background:#eaf4fe;border:1px solid #b0d4f1;"
            "border-radius:6px;padding:8px 12px;font-size:10pt;color:#1a5276;}")
        guide_row.addWidget(self.guidance_label, 1)
        guide_row.addWidget(
            self._help_btn(_HELP_OVERVIEW(self._app_name),
                           f"{self._app_name} — Overview"))
        root.addLayout(guide_row)

        splitter = QSplitter(_Vertical)
        root.addWidget(splitter)

        # ── Top panel ─────────────────────────────────────────────────
        top = QWidget()
        top_lay = QHBoxLayout(top)

        left_col = QVBoxLayout()

        # Data Sources
        self.dg = QGroupBox(self._profile["data_sources_title"])
        dgrid = QGridLayout(self.dg)
        dgrid.addWidget(QLabel("Left dataset:"), 0, 0)
        self.lbl_left = QLabel("(none)")
        self.lbl_left.setStyleSheet("color:grey")
        dgrid.addWidget(self.lbl_left, 0, 1)
        self.btn_load_left = QPushButton("Load …")
        dgrid.addWidget(self.btn_load_left, 0, 2)

        # Right dataset row — hidden for Dedupe
        self.right_dataset_row_widgets = []
        lbl_right_title = QLabel("Right dataset:")
        self.lbl_right = QLabel("(none)")
        self.lbl_right.setStyleSheet("color:grey")
        self.btn_load_right = QPushButton("Load …")
        dgrid.addWidget(lbl_right_title, 1, 0)
        dgrid.addWidget(self.lbl_right, 1, 1)
        dgrid.addWidget(self.btn_load_right, 1, 2)
        self.right_dataset_row_widgets = [
            lbl_right_title, self.lbl_right, self.btn_load_right]

        left_col.addWidget(self.dg)

        # Column Mapping
        mg = QGroupBox("② Column Mapping")
        mlay = QVBoxLayout(mg)
        btn_row = QHBoxLayout()
        self.btn_auto_map   = QPushButton("Auto Map")
        self.btn_add_map    = QPushButton("Add Mapping")
        self.btn_remove_map = QPushButton("Remove Selected")
        btn_row.addWidget(self.btn_auto_map)
        btn_row.addWidget(self.btn_add_map)
        btn_row.addWidget(self.btn_remove_map)
        btn_row.addWidget(self._help_btn(HELP_COLUMN_MAPPING))
        mlay.addLayout(btn_row)
        self.mapping_table = QTableWidget(0, 4)
        self.mapping_table.setHorizontalHeaderLabels(
            ["Left Column", "Right Column", "Comparison Type", "Block?"])
        self.mapping_table.horizontalHeader().setSectionResizeMode(_Stretch)
        mlay.addWidget(self.mapping_table)
        left_col.addWidget(mg)
        top_lay.addLayout(left_col, 3)

        # Right column — settings + actions
        right_col = QVBoxLayout()

        sg = QGroupBox("③ Settings")
        sgrid = QGridLayout(sg)
        sgrid.addWidget(QLabel("Match threshold:"), 0, 0)
        self.spin_threshold = QDoubleSpinBox()
        self.spin_threshold.setRange(0.01, 0.99)
        self.spin_threshold.setSingleStep(0.05)
        self.spin_threshold.setValue(0.50)
        sgrid.addWidget(self.spin_threshold, 0, 1)

        sgrid.addWidget(QLabel("Max pairs (u est.):"), 1, 0)
        self.spin_max_pairs = QSpinBox()
        self.spin_max_pairs.setRange(10_000, 10_000_000)
        self.spin_max_pairs.setSingleStep(100_000)
        self.spin_max_pairs.setValue(1_000_000)
        sgrid.addWidget(self.spin_max_pairs, 1, 1)

        dedup_row = QHBoxLayout()
        self.chk_dedupe = QCheckBox("Single-table mode (deduplicate)")
        dedup_row.addWidget(self.chk_dedupe)
        dedup_row.addStretch()
        sgrid.addLayout(dedup_row, 2, 0, 1, 3)
        right_col.addWidget(sg)

        # AutoRecon: Reconciliation Settings group
        self.recon_group = QGroupBox("④ Reconciliation Settings")
        recon_grid = QGridLayout(self.recon_group)
        recon_grid.addWidget(QLabel("Amount column:"), 0, 0)
        self.combo_amount_col = QComboBox()
        self.combo_amount_col.setMinimumWidth(180)
        recon_grid.addWidget(self.combo_amount_col, 0, 1)
        recon_grid.addWidget(QLabel("Recon tolerance:"), 1, 0)
        self.spin_recon_tol = QDoubleSpinBox()
        self.spin_recon_tol.setRange(0.0, 1000.0)
        self.spin_recon_tol.setDecimals(4)
        self.spin_recon_tol.setValue(0.01)
        recon_grid.addWidget(self.spin_recon_tol, 1, 1)
        recon_grid.addWidget(
            self._help_btn(HELP_RECON_SETTINGS, f"{self._app_name} — Recon Settings"),
            0, 2)
        right_col.addWidget(self.recon_group)

        # Actions
        action_label = "⑤ Actions" if self._profile["has_recon_step"] else "④ Actions"
        ag = QGroupBox(action_label)
        alay = QVBoxLayout(ag)
        run_row = QHBoxLayout()
        self.btn_run = QPushButton(self._profile["run_button_label"])
        self.btn_run.setStyleSheet(
            "QPushButton{background:#4CAF50;color:white;font-weight:bold;padding:8px}"
            "QPushButton:hover{background:#45a049}"
            "QPushButton:disabled{background:#bdc3c7;color:#7f8c8d}")
        run_row.addWidget(self.btn_run)
        alay.addLayout(run_row)

        self.btn_stop = QPushButton("■  Stop")
        self.btn_stop.setEnabled(False)
        alay.addWidget(self.btn_stop)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setVisible(False)
        alay.addWidget(self.progress_bar)

        self.lbl_elapsed = QLabel("Elapsed: —")
        self.lbl_elapsed.setStyleSheet("color:#555;font-size:9pt;")
        alay.addWidget(self.lbl_elapsed)

        erow = QHBoxLayout()
        self.btn_export     = QPushButton("Export Excel …")
        self.btn_export_csv = QPushButton("Export CSV …")
        self.btn_export.setEnabled(False)
        self.btn_export_csv.setEnabled(False)
        erow.addWidget(self.btn_export)
        erow.addWidget(self.btn_export_csv)
        alay.addLayout(erow)

        self.btn_reset = QPushButton("🔄  Reset All")
        alay.addWidget(self.btn_reset)
        right_col.addWidget(ag)

        self.lbl_trial = QLabel("")
        self.lbl_trial.setStyleSheet(
            "color:#c0392b;font-size:9pt;font-style:italic;padding:4px;")
        right_col.addWidget(self.lbl_trial)
        right_col.addStretch()
        top_lay.addLayout(right_col, 2)
        splitter.addWidget(top)

        # ── Bottom: tabs ──────────────────────────────────────────────
        self.tabs = QTabWidget()

        # Output Log tab
        log_w = QWidget()
        log_l = QVBoxLayout(log_w)
        log_h = QHBoxLayout()
        log_h.addWidget(QLabel("Output Log:"))
        self.btn_clear_log = QPushButton("Clear")
        self.btn_clear_log.setFixedWidth(60)
        log_h.addWidget(self.btn_clear_log)
        log_h.addStretch()
        log_l.addLayout(log_h)
        self.outputText = QTextEdit()
        self.outputText.setReadOnly(True)
        self.outputText.setFont(QFont("Consolas", 9))
        log_l.addWidget(self.outputText)
        self.tabs.addTab(log_w, "Output Log")

        # Results Preview tab (linkage / dedupe results)
        res_w = QWidget()
        res_l = QVBoxLayout(res_w)
        res_h = QHBoxLayout()
        res_h.addWidget(QLabel("Matched record pairs:"))
        self.result_count_label = QLabel("")
        self.result_count_label.setStyleSheet("color:#444;font-weight:bold;")
        res_h.addWidget(self.result_count_label)
        res_h.addStretch()
        res_l.addLayout(res_h)
        self.results_table = QTableWidget()
        self.results_table.setEditTriggers(_NoEdit)
        res_l.addWidget(self.results_table)
        self.tabs.addTab(res_w, "Results Preview")

        # Visualisation tabs (Match Weights, M/U Parameters, Waterfall)
        self.viz_tabs: Dict[str, dict] = {}
        for name in ("Match Weights", "M/U Parameters", "Waterfall"):
            container = QWidget()
            vlay = QVBoxLayout(container)
            vh = QHBoxLayout()
            vh.addWidget(QLabel(f"{name}:"))
            vh.addStretch()
            vlay.addLayout(vh)

            btn_b = QPushButton(f"🌐  Open '{name}' in browser")
            btn_b.setVisible(False)
            vlay.addWidget(btn_b)

            canvas = None
            if HAS_MATPLOTLIB and FigureCanvas:
                canvas = FigureCanvas(Figure(figsize=(8, 4)))
                canvas.setMinimumHeight(250)
                vlay.addWidget(canvas)

            web = None
            if HAS_WEBENGINE:
                web = QWebEngineView()
                web.setMinimumHeight(300)
                vlay.addWidget(web)

            self.tabs.addTab(container, name)
            self.viz_tabs[name] = {
                "widget": container, "layout": vlay,
                "btn_browser": btn_b, "canvas": canvas,
                "web": web, "_html": None,
            }

        # AutoRecon: Recon Results tab
        recon_tab_w = QWidget()
        recon_tab_l = QVBoxLayout(recon_tab_w)
        rh = QHBoxLayout()
        rh.addWidget(QLabel("Reconciled rows (recon_group column added):"))
        rh.addStretch()
        recon_tab_l.addLayout(rh)
        self.recon_result_table = QTableView()
        recon_tab_l.addWidget(self.recon_result_table)
        self.recon_tab_index = self.tabs.addTab(recon_tab_w, "Recon Results")

        splitter.addWidget(self.tabs)
        splitter.setSizes([400, 350])

    # ══════════════════════════════════════════════════════════════════════
    #  Profile-driven visibility
    # ══════════════════════════════════════════════════════════════════════

    def _apply_profile_visibility(self):
        p = self._profile

        # Right dataset row
        for w in self.right_dataset_row_widgets:
            w.setVisible(p["show_right_dataset"])

        # Dedupe checkbox
        self.chk_dedupe.setVisible(p["show_dedupe_checkbox"])
        if p["force_dedupe_mode"]:
            self.chk_dedupe.setChecked(True)
            self.chk_dedupe.setEnabled(False)
            # In Dedupe app, right dataset is never used
            self.btn_load_right.setEnabled(False)
            self.lbl_right.setText("(same as left)")
            self.lbl_right.setStyleSheet("color:grey;font-style:italic")

        # Recon Settings group
        self.recon_group.setVisible(p["has_recon_step"])

        # Recon Results tab
        self.tabs.setTabVisible(self.recon_tab_index, p["has_recon_step"])

    # ══════════════════════════════════════════════════════════════════════
    #  Guidance
    # ══════════════════════════════════════════════════════════════════════

    def _update_guidance(self):
        p = self._profile
        has_left     = self.df_left is not None
        has_right    = self.df_right is not None or self._is_dedupe()
        has_mappings = self.mapping_table.rowCount() > 0
        has_results  = (self.df_results is not None
                        and not self.df_results.empty)

        if not has_left:
            msg = ("👋 <b>Step 1:</b>  Load your dataset using "
                   "the <b>Load …</b> buttons.")
        elif not has_right and not self._is_dedupe():
            msg = "📂 <b>Step 1:</b>  Now load the <b>Right Dataset</b>."
        elif not has_mappings:
            msg = ("📋 <b>Step 2:</b>  Map columns — click "
                   "<b>Auto Map</b> or <b>Add Mapping</b>.")
        elif not has_results:
            msg = (f"🚀 <b>Step 3:</b>  Click "
                   f"<b>{p['run_button_label']}</b> to begin.")
        else:
            n = len(self.df_results)
            msg = (f"✅ <b>Done!</b>  {n:,} matched pair(s) found.  "
                   f"Use <b>Export</b> to save.")

        self.guidance_label.setText(msg)

    # ══════════════════════════════════════════════════════════════════════
    #  Signal connections
    # ══════════════════════════════════════════════════════════════════════

    def _connect_signals(self):
        self.btn_load_left.clicked.connect(self._on_load_left)
        self.btn_load_right.clicked.connect(self._on_load_right)
        self.btn_auto_map.clicked.connect(self._auto_map)
        self.btn_add_map.clicked.connect(self._add_mapping)
        self.btn_remove_map.clicked.connect(self._remove_mapping)
        self.btn_run.clicked.connect(self._on_run)
        self.btn_stop.clicked.connect(self._on_stop)
        self.btn_export.clicked.connect(self._on_export)
        self.btn_export_csv.clicked.connect(self._on_export_csv)
        self.btn_reset.clicked.connect(self._on_reset_all)
        self.chk_dedupe.toggled.connect(self._on_dedup_toggled)
        self.btn_clear_log.clicked.connect(lambda: self.outputText.clear())

        for tab_name, info in self.viz_tabs.items():
            info["btn_browser"].clicked.connect(
                lambda _c=False, n=tab_name: self._open_chart_in_browser(n))

    # ══════════════════════════════════════════════════════════════════════
    #  Helpers
    # ══════════════════════════════════════════════════════════════════════

    def _is_dedupe(self) -> bool:
        return self._profile["force_dedupe_mode"] or self.chk_dedupe.isChecked()

    def _both_sides_loaded(self) -> bool:
        if self.df_left is None:
            return False
        if self._is_dedupe():
            return True
        return self.df_right is not None

    def _show_error(self, msg: str):
        dlg = QMessageBox(self)
        dlg.setWindowTitle(f"{self._app_name} — Error")
        dlg.setIcon(_Warning)
        dlg.setText(msg)
        dlg.addButton(_Ok)
        btn_r = dlg.addButton("Reset All", _ResetRole)
        exec_dialog(dlg)
        if dlg.clickedButton() == btn_r:
            self._on_reset_all()

    def _log(self, text: str):
        self.outputText.append(text)
        QApplication.processEvents()

    def _load_file(self, title: str):
        path, _ = QFileDialog.getOpenFileName(
            self, title, "",
            "Data files (*.csv *.xlsx *.xls *.parquet);;All files (*)")
        if not path:
            return None
        p = Path(path)
        try:
            ext = p.suffix.lower()
            if ext == ".csv":
                df = pd.read_csv(path)
            elif ext in (".xlsx", ".xls"):
                df = pd.read_excel(path)
            elif ext == ".parquet":
                df = pd.read_parquet(path)
            else:
                df = pd.read_csv(path)

            limit = self._profile["free_trial_row_limit"]
            if app_config.FREE_TRIAL and len(df) > limit:
                dlg = QMessageBox(self)
                dlg.setIcon(_Warning)
                dlg.setWindowTitle("Trial Limit")
                dlg.setTextFormat(_RichText)
                dlg.setText(
                    f"<h3>Trial Limit Exceeded</h3>"
                    f"<p>Free trial allows up to <b>{limit}</b> rows.  "
                    f"This file has <b>{len(df):,}</b> rows.</p>"
                    f"<p>Purchase a licence at "
                    f"<a href='https://zolvertechnology.wixsite.com/home'>"
                    f"zolvertechnology.wixsite.com/home</a></p>")
                dlg.setTextInteractionFlags(_TextBrowser)
                exec_dialog(dlg)
                return None

            self._log(f"Loaded {p.name}  ({len(df):,} rows × {len(df.columns)} cols)")
            return df, path
        except Exception as e:
            self._show_error(f"Could not load <b>{p.name}</b>:\n\n{e}")
            return None

    def _left_columns(self):
        return list(self.df_left.columns) if self.df_left is not None else []

    def _right_columns(self):
        if self._is_dedupe():
            return self._left_columns()
        return list(self.df_right.columns) if self.df_right is not None else []

    def _numeric_columns(self, df: pd.DataFrame) -> List[str]:
        """Return columns that are purely numeric."""
        result = []
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                result.append(col)
            else:
                try:
                    pd.to_numeric(df[col], errors="raise")
                    result.append(col)
                except (ValueError, TypeError):
                    pass
        return result

    def _refresh_amount_combo(self):
        """Populate the amount column combo with numeric columns from loaded data."""
        self.combo_amount_col.clear()
        df = self.df_left if self.df_left is not None else None
        if df is None:
            return
        num_cols = self._numeric_columns(df)
        for c in num_cols:
            self.combo_amount_col.addItem(c, c)

    # ── Mapping table ──────────────────────────────────────────────────

    def _read_mappings(self) -> List[Dict]:
        out = []
        for row in range(self.mapping_table.rowCount()):
            lc = self.mapping_table.cellWidget(row, 0)
            rc = self.mapping_table.cellWidget(row, 1)
            tc = self.mapping_table.cellWidget(row, 2)
            bw = self.mapping_table.cellWidget(row, 3)
            if lc and rc and tc:
                chk = bw.findChild(QCheckBox) if bw else None
                out.append({
                    "left":  lc.currentText(),
                    "right": rc.currentText(),
                    "type":  tc.currentText(),
                    "block": chk.isChecked() if chk else True,
                })
        return out

    def _make_block_checkbox(self, checked=True):
        w = QWidget()
        lay = QHBoxLayout(w)
        lay.setContentsMargins(0, 0, 0, 0)
        chk = QCheckBox()
        chk.setChecked(checked)
        lay.addWidget(chk)
        lay.setAlignment(_AlignCenter)
        return w

    def _insert_mapping_row(self, left_col="", right_col="",
                            cmp_type="Exact", block=True):
        row = self.mapping_table.rowCount()
        self.mapping_table.insertRow(row)

        lcb = QComboBox()
        lcb.addItems(self._left_columns())
        if left_col:
            lcb.setCurrentText(left_col)
        self.mapping_table.setCellWidget(row, 0, lcb)

        rcb = QComboBox()
        rcb.addItems(self._right_columns())
        if right_col:
            rcb.setCurrentText(right_col)
        self.mapping_table.setCellWidget(row, 1, rcb)

        tcb = QComboBox()
        tcb.addItems(["Exact", "Levenshtein", "Jaro-Winkler", "Date"])
        tcb.setCurrentText(cmp_type)
        self.mapping_table.setCellWidget(row, 2, tcb)
        self.mapping_table.setCellWidget(row, 3, self._make_block_checkbox(block))

    def _clear_mappings_and_results(self, msg=""):
        self.mapping_table.setRowCount(0)
        self.df_results = None
        self.linker     = None
        self.charts     = {}
        self.results_table.setRowCount(0)
        self.results_table.setColumnCount(0)
        self.result_count_label.setText("")
        self.btn_export.setEnabled(False)
        self.btn_export_csv.setEnabled(False)
        for info in self.viz_tabs.values():
            if info["web"]:
                info["web"].setHtml("")
            if info["canvas"]:
                info["canvas"].figure.clear()
                info["canvas"].draw()
            info["btn_browser"].setVisible(False)
            info["_html"] = None
        if msg:
            self._log(f"⚠ {msg}")

    # ══════════════════════════════════════════════════════════════════════
    #  Slots — data loading
    # ══════════════════════════════════════════════════════════════════════

    @safe_slot
    def _on_load_left(self):
        had = self._both_sides_loaded() and self.mapping_table.rowCount() > 0
        result = self._load_file("Load Left Dataset")
        if result:
            self.df_left, path = result
            self.lbl_left.setText(Path(path).name)
            self.lbl_left.setStyleSheet("color:black")
            if had:
                self._clear_mappings_and_results(
                    "Dataset reloaded — mappings cleared.")
            if self._profile["has_recon_step"]:
                self._refresh_amount_combo()
            self._update_guidance()

    @safe_slot
    def _on_load_right(self):
        had = self._both_sides_loaded() and self.mapping_table.rowCount() > 0
        result = self._load_file("Load Right Dataset")
        if result:
            self.df_right, path = result
            self.lbl_right.setText(Path(path).name)
            self.lbl_right.setStyleSheet("color:black")
            if had:
                self._clear_mappings_and_results(
                    "Dataset reloaded — mappings cleared.")
            self._update_guidance()

    @safe_slot
    def _on_dedup_toggled(self, checked):
        if checked:
            self.lbl_right.setText("(same as left)")
            self.lbl_right.setStyleSheet("color:grey;font-style:italic")
            self.btn_load_right.setEnabled(False)
        else:
            has = self.df_right is not None
            self.lbl_right.setText("(none)" if not has else self.lbl_right.text())
            self.lbl_right.setStyleSheet("color:black" if has else "color:grey")
            self.btn_load_right.setEnabled(True)
        if self.mapping_table.rowCount() > 0:
            self._clear_mappings_and_results("Mode changed — mappings cleared.")
        self._update_guidance()

    # ══════════════════════════════════════════════════════════════════════
    #  Slots — mapping
    # ══════════════════════════════════════════════════════════════════════

    @safe_slot
    def _add_mapping(self):
        if self.df_left is None:
            self._show_error("Load the left dataset first.")
            return
        if not self._is_dedupe() and self.df_right is None:
            self._show_error("Load both datasets first.")
            return
        self._insert_mapping_row()
        self._update_guidance()

    @safe_slot
    def _auto_map(self):
        if self.df_left is None:
            self._show_error("Load the left dataset first.")
            return
        if not self._is_dedupe() and self.df_right is None:
            self._show_error("Load both datasets first.")
            return
        self.mapping_table.setRowCount(0)
        lc = self._left_columns()
        rc = self._right_columns()
        norm = lambda s: s.strip().lower().replace("_", "").replace(" ", "")
        for l_col in lc:
            for r_col in rc:
                if norm(l_col) == norm(r_col):
                    n = norm(l_col)
                    t = ("Date" if any(k in n for k in ("date", "dob", "birth"))
                         else "Jaro-Winkler"
                         if any(k in n for k in ("name", "first", "last",
                                                  "surname", "address", "city"))
                         else "Exact")
                    self._insert_mapping_row(l_col, r_col, t, True)
                    break
        self._log(f"Auto-mapped {self.mapping_table.rowCount()} column pair(s).")
        self._update_guidance()

    @safe_slot
    def _remove_mapping(self):
        rows = sorted(
            {idx.row() for idx in self.mapping_table.selectedIndexes()},
            reverse=True)
        if not rows:
            QMessageBox.information(self, "Remove", "Select a row first.")
            return
        for r in rows:
            self.mapping_table.removeRow(r)
        self._update_guidance()

    # ══════════════════════════════════════════════════════════════════════
    #  Slots — run / stop
    # ══════════════════════════════════════════════════════════════════════

    @safe_slot
    def _on_run(self):
        if SPLINK_VERSION == 0:
            self._show_error("Splink is not installed.\n\n  pip install splink")
            return
        if self.df_left is None:
            self._show_error("Load the left dataset first.")
            return
        if not self._is_dedupe() and self.df_right is None:
            self._show_error("Load the right dataset or enable single-table mode.")
            return
        mappings = self._read_mappings()
        if not mappings:
            self._show_error("Add at least one column mapping.")
            return

        # AutoRecon: validate amount column
        if self._profile["has_recon_step"]:
            amount_col = self.combo_amount_col.currentData()
            if not amount_col:
                self._show_error("Select an amount column for reconciliation.")
                return

        has_blocking = any(m.get("block") for m in mappings)
        if not has_blocking:
            reply = QMessageBox.question(
                self, "No Blocking",
                "No blocking rules set — every record will be compared to every "
                "other record.  This can be very slow.\n\nContinue?",
                _Yes | _No, _No)
            if reply != _Yes:
                return

        # Prepare dataframes
        df_l = self.df_left.copy()
        df_r = (self.df_left.copy() if self._is_dedupe()
                else self.df_right.copy())

        rename = {m["right"]: m["left"]
                  for m in mappings if m["right"] != m["left"]}
        if rename:
            df_r = df_r.rename(columns=rename)
            self._log(f"Renamed right columns: {rename}")

        col_names = [m["left"] for m in mappings]
        settings  = build_splink_settings(mappings, self._is_dedupe())

        self._set_running(True)
        import time
        self._start_time = time.time()
        self.timer.start(200)

        self._log("═" * 60)
        self._log(f"Starting {self._app_name} …")

        self.link_worker = LinkWorker(
            df_l, df_r, settings, col_names,
            threshold=self.spin_threshold.value(),
            max_pairs=self.spin_max_pairs.value(),
            dedupe=self._is_dedupe(),
            run_recon=self._profile["has_recon_step"],
        )
        self.link_worker.progress.connect(self._log)
        self.link_worker.finished.connect(self._on_link_done)
        self.link_worker.error.connect(self._on_worker_error)

        if self._profile["has_recon_step"]:
            self.link_worker.recon_ready.connect(self._on_recon_ready)

        self.link_worker.start()

    @safe_slot
    def _on_stop(self):
        if self.link_worker:
            self.link_worker.stop()
        if self.recon_worker:
            self.recon_worker.stop()
        self._log("Stop requested …")

    def _set_running(self, running: bool):
        self.btn_run.setEnabled(not running)
        self.btn_stop.setEnabled(running)
        self.progress_bar.setVisible(running)
        if not running:
            self.timer.stop()
            self._update_elapsed()

    def _update_elapsed(self):
        if self._start_time is None:
            return
        import time
        e = time.time() - self._start_time
        self.lbl_elapsed.setText(f"Elapsed: {e:.1f}s")

    # ══════════════════════════════════════════════════════════════════════
    #  Worker callbacks — Splink
    # ══════════════════════════════════════════════════════════════════════

    def _on_link_done(self, result: dict):
        self.df_results = result["df_results"]
        self.charts     = result.get("charts", {})
        self.linker     = result.get("linker")

        # For AutoRecon, don't stop the running state yet — recon step follows
        if not self._profile["has_recon_step"]:
            self._set_running(False)
            self.btn_export.setEnabled(True)
            self.btn_export_csv.setEnabled(True)
            self.statusBar().showMessage(
                f"Done — {len(self.df_results):,} matches.")

        self._log(f"✓ Linkage complete — {len(self.df_results):,} pairs.")
        self._populate_results_table()
        self._render_visualisations()
        self._update_guidance()

    def _on_recon_ready(self, result: dict):
        """Triggered (AutoRecon only) after Splink finishes."""
        amount_col = self.combo_amount_col.currentData()
        mappings   = self._read_mappings()
        col_names  = [m["left"] for m in mappings]
        recon_tol  = self.spin_recon_tol.value()

        self._log("Starting reconciliation step …")
        self.recon_worker = ReconWorker(
            df_results = result["df_results"],
            df_l       = result["df_l"],
            df_r       = result["df_r"],
            amount_col = amount_col,
            mapped_cols = col_names,
            recon_tol  = recon_tol,
        )
        self.recon_worker.progress.connect(self._log)
        self.recon_worker.finished.connect(self._on_recon_done)
        self.recon_worker.error.connect(self._on_worker_error)
        self.recon_worker.start()

    def _on_recon_done(self, df_recon: pd.DataFrame):
        self._set_running(False)
        self.btn_export.setEnabled(True)
        self.btn_export_csv.setEnabled(True)

        model = LazyTableModel(df_recon)
        self.recon_result_table.setModel(model)
        self.recon_result_table.verticalScrollBar().valueChanged.connect(
            lambda v: model.loadMore() if v >= self.recon_result_table.verticalScrollBar().maximum() else None)

        self.tabs.setCurrentIndex(self.recon_tab_index)
        n_groups = df_recon["recon_group"].replace("", pd.NA).dropna().nunique()
        self._log(f"✓ AutoRecon complete — {n_groups} zero-sum group(s) found.")
        self.statusBar().showMessage(
            f"AutoRecon done — {n_groups} zero-sum group(s).")
        self._update_guidance()

    def _on_worker_error(self, msg: str):
        self._set_running(False)
        self._log(f"[ERROR] {msg}")
        self._show_error(f"Operation failed:\n\n{msg}")

    # ══════════════════════════════════════════════════════════════════════
    #  Results table
    # ══════════════════════════════════════════════════════════════════════

    def _populate_results_table(self):
        if self.df_results is None:
            return
        df = self.df_results.head(500).reset_index(drop=True)
        nr, nc = df.shape
        self.results_table.setRowCount(nr)
        self.results_table.setColumnCount(nc)
        self.results_table.setHorizontalHeaderLabels([str(c) for c in df.columns])
        for i in range(nr):
            for j in range(nc):
                self.results_table.setItem(
                    i, j, QTableWidgetItem(str(df.iat[i, j])))
        self.result_count_label.setText(
            f"Showing {nr:,} of {len(self.df_results):,} pair(s)")

    # ══════════════════════════════════════════════════════════════════════
    #  Visualisations
    # ══════════════════════════════════════════════════════════════════════

    def _render_visualisations(self):
        key_map = {
            "Match Weights":  "match_weights",
            "M/U Parameters": "m_u_parameters",
            "Waterfall":      "waterfall",
        }
        for tab_name, chart_key in key_map.items():
            chart_obj = self.charts.get(chart_key)
            if chart_obj is not None:
                self._render_one_chart(tab_name, chart_obj)
            else:
                self._render_matplotlib_fallback(tab_name)

    def _render_one_chart(self, tab_name: str, chart_obj):
        info = self.viz_tabs[tab_name]
        html = chart_to_html(chart_obj)
        if html is None:
            self._render_matplotlib_fallback(tab_name)
            return
        info["_html"] = html
        if HAS_WEBENGINE and info["web"] is not None:
            try:
                info["web"].setHtml(html)
                info["web"].setVisible(True)
                if info["canvas"]:
                    info["canvas"].setVisible(False)
                info["btn_browser"].setVisible(True)
                return
            except Exception as e:
                self._log(f"⚠ WebEngine error for '{tab_name}': {e}")
        self._render_matplotlib_fallback(tab_name)
        info["btn_browser"].setVisible(True)

    def _render_matplotlib_fallback(self, tab_name: str):
        info = self.viz_tabs[tab_name]
        canvas = info.get("canvas")
        if canvas is None or self.df_results is None:
            info["btn_browser"].setVisible(bool(info.get("_html")))
            return
        fig = canvas.figure
        fig.clear()
        ax = fig.add_subplot(111)
        if "match_probability" in self.df_results.columns:
            data = self.df_results["match_probability"].dropna()
            ax.hist(data, bins=50, edgecolor="black", alpha=0.75, color="#4CAF50")
            ax.set_xlabel("Match Probability")
            ax.set_ylabel("Frequency")
            ax.set_title(tab_name)
        else:
            ax.text(0.5, 0.5, f"No data for {tab_name}",
                    ha="center", va="center", transform=ax.transAxes,
                    fontsize=14, color="grey")
        fig.tight_layout()
        canvas.draw()
        canvas.setVisible(True)
        if info["web"]:
            info["web"].setVisible(False)
        info["btn_browser"].setVisible(bool(info.get("_html")))

    @safe_slot
    def _open_chart_in_browser(self, chart_name: str):
        info = self.viz_tabs.get(chart_name)
        if not info or not info.get("_html"):
            self._show_error(f"No chart HTML for '{chart_name}'.")
            return
        try:
            open_html_in_browser(info["_html"], f"{self._app_name}_chart_")
            self._log(f"Opened '{chart_name}' in browser.")
        except Exception as e:
            self._show_error(f"Could not open browser:\n{e}")

    # ══════════════════════════════════════════════════════════════════════
    #  Export
    # ══════════════════════════════════════════════════════════════════════

    @safe_slot
    def _on_export(self):
        df = self._export_dataframe()
        if df is None:
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Results", f"{self._app_name}_results.xlsx",
            "Excel files (*.xlsx);;All files (*)")
        if path:
            try:
                df.to_excel(path, index=False)
                self._log(f"Exported {len(df):,} rows → {path}")
                QMessageBox.information(self, "Export", f"Saved:\n{path}")
            except Exception as e:
                self._show_error(f"Export failed:\n{e}")

    @safe_slot
    def _on_export_csv(self):
        df = self._export_dataframe()
        if df is None:
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Results", f"{self._app_name}_results.csv",
            "CSV files (*.csv);;All files (*)")
        if path:
            try:
                df.to_csv(path, index=False)
                self._log(f"Exported {len(df):,} rows → {path}")
                QMessageBox.information(self, "Export", f"Saved:\n{path}")
            except Exception as e:
                self._show_error(f"Export failed:\n{e}")

    def _export_dataframe(self) -> Optional[pd.DataFrame]:
        if self._profile["has_recon_step"]:
            # For AutoRecon export the recon result table's model if available
            model = self.recon_result_table.model()
            if model and hasattr(model, "_df"):
                return model._df
        if self.df_results is None or self.df_results.empty:
            self._show_error("No results to export — run first.")
            return None
        return self.df_results

    # ══════════════════════════════════════════════════════════════════════
    #  Reset
    # ══════════════════════════════════════════════════════════════════════

    @safe_slot
    def _on_reset_all(self):
        reply = QMessageBox.question(
            self, "Reset All",
            "Clear all data, mappings, results and visualisations?",
            _Yes | _No, _No)
        if reply != _Yes:
            return

        for w in (self.link_worker, self.recon_worker):
            if w and w.isRunning():
                w.stop()
                w.wait(3000)

        self.df_left = self.df_right = self.df_results = None
        self.linker  = None
        self.charts  = {}
        self.link_worker  = None
        self.recon_worker = None

        self.lbl_left.setText("(none)")
        self.lbl_left.setStyleSheet("color:grey")
        self.lbl_right.setText("(none)")
        self.lbl_right.setStyleSheet("color:grey")
        self.mapping_table.setRowCount(0)
        self.results_table.setRowCount(0)
        self.results_table.setColumnCount(0)
        self.result_count_label.setText("")
        self.outputText.clear()
        self.lbl_elapsed.setText("Elapsed: —")
        self.btn_export.setEnabled(False)
        self.btn_export_csv.setEnabled(False)
        self.btn_run.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.progress_bar.setVisible(False)

        if not self._profile["force_dedupe_mode"]:
            self.chk_dedupe.setChecked(False)
        if self._profile["show_right_dataset"]:
            self.btn_load_right.setEnabled(True)

        for info in self.viz_tabs.values():
            if info["web"]:
                info["web"].setHtml("")
                info["web"].setVisible(True)
            if info["canvas"]:
                info["canvas"].figure.clear()
                info["canvas"].draw()
                info["canvas"].setVisible(True)
            info["btn_browser"].setVisible(False)
            info["_html"] = None

        self.recon_result_table.setModel(None)
        self._log("Reset complete — ready for a fresh start.")
        self.statusBar().showMessage(self._profile["status_ready"])
        self._update_guidance()

    def closeEvent(self, event):
        for w in (self.link_worker, self.recon_worker):
            if w and w.isRunning():
                w.stop()
                w.wait(3000)
        event.accept()
        
# ═══════════════════════════════════════════════════════════════════════════════
#  Shared entry point — called by every run_*.py launcher
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    import sys, os, ctypes, traceback

    profile  = get_profile()
    app_name = get_app_name()
    version  = profile["version"]

    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
            f"{app_name}_{version}")
    except (ImportError, AttributeError, OSError):
        pass

    app = QApplication(sys.argv)
    app.setApplicationName(app_name)
    app.setStyle("Fusion")

    try:
        icon_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), app_name + ".png")
        if os.path.exists(icon_path):
            from qt_compat import QIcon
            app.setWindowIcon(QIcon(icon_path))
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

    if SPLINK_VERSION == 0:
        QMessageBox.warning(
            None, f"{app_name} — Missing Dependency",
            "Splink is not installed.\n\n  pip install splink duckdb\n\n"
            "The application will open but matching will not work.")

    window = MainWindow()

    if app_config.FREE_TRIAL:
        limit = profile["free_trial_row_limit"]
        window.lbl_trial.setText(
            f"Free trial — {limit} row limit per dataset")
        window._log(
            f"Free trial mode (limit: {limit} rows per dataset).")
    else:
        window.lbl_trial.setText("")
        window._log("Licensed — no row limits.")

    if SPLINK_VERSION > 0:
        window._log(f"Splink v{SPLINK_VERSION} detected.")
    else:
        window._log("⚠  Splink not found — pip install splink")
    if not HAS_WEBENGINE:
        window._log(
            "⚠  QtWebEngine not found — charts use matplotlib / browser.  "
            "pip install PyQt5-WebEngine  (or PyQt6-WebEngine)")

    def _global_hook(exc_type, exc_value, exc_tb):
        tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        try:
            window.outputText.append(f"[UNHANDLED ERROR]\n{tb_str}")
        except Exception:
            pass
        QMessageBox.critical(
            window, f"{app_name} — Unexpected Error",
            f"An unexpected error occurred:\n\n{exc_value}\n\n"
            f"Use 'Reset All' if the state looks wrong.\n\n"
            f"Details are in the Output Log.")

    sys.excepthook = _global_hook
    window.show()
    sys.exit(exec_app(app))