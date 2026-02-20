"""
Splink probabilistic linkage / dedupe engine.
Contains: PyQt worker thread, Vega-Lite chart helpers, safe_slot decorator.
"""

import os
import sys
import json
import tempfile
import webbrowser
import traceback
import functools
import inspect
from typing import Optional, Dict, Any, List

import pandas as pd

from qt_compat import (
    PYQT, PYQT as _PYQT, HAS_WEBENGINE, HAS_MATPLOTLIB,
    FigureCanvas, Figure,
    QThread, pyqtSignal, QApplication,
    _AlignCenter,
)

# ── Splink version detection ───────────────────────────────────────────────────
SPLINK_VERSION = 0

try:
    from splink import Linker, SettingsCreator, DuckDBAPI, block_on
    import splink.comparison_library as cl
    SPLINK_VERSION = 4
except ImportError:
    try:
        from splink.duckdb.linker import DuckDBLinker
        import splink.comparison_library as cl
        SPLINK_VERSION = 3
    except ImportError:
        pass

DEBUG = os.environ.get("RECORDLINK_DEBUG", "0") == "1"

# ── JavaScript polyfills for older QtWebEngine ─────────────────────────────────
JS_POLYFILLS = """\
<script>
if (typeof Object.hasOwn !== 'function') {
    Object.defineProperty(Object, 'hasOwn', {
        value: function(obj, prop) {
            return Object.prototype.hasOwnProperty.call(obj, prop);
        }, writable: true, configurable: true, enumerable: false
    });
}
if (typeof structuredClone !== 'function') {
    window.structuredClone = function(v) { return JSON.parse(JSON.stringify(v)); };
}
if (!Array.prototype.at) {
    Object.defineProperty(Array.prototype, 'at', {
        value: function(n) {
            n = Math.trunc(n) || 0; if (n < 0) n += this.length;
            return (n < 0 || n >= this.length) ? undefined : this[n];
        }, writable: true, configurable: true
    });
}
if (!String.prototype.replaceAll) {
    String.prototype.replaceAll = function(s, r) { return this.split(s).join(r); };
}
if (!Object.fromEntries) {
    Object.fromEntries = function(iter) {
        var o = {}; for (var p of iter) { o[p[0]] = p[1]; } return o;
    };
}
if (typeof globalThis === 'undefined') { window.globalThis = window; }
</script>
"""

VEGA_CDN        = "https://cdn.jsdelivr.net/npm/vega@5.21.0/build/vega.min.js"
VEGA_LITE_V5    = "https://cdn.jsdelivr.net/npm/vega-lite@5.2.0/build/vega-lite.min.js"
VEGA_LITE_V4    = "https://cdn.jsdelivr.net/npm/vega-lite@4.17.0/build/vega-lite.min.js"
VEGA_EMBED_CDN  = "https://cdn.jsdelivr.net/npm/vega-embed@6.18.2/build/vega-embed.min.js"


def inject_polyfills(html: str) -> str:
    low = html.lower()
    if "<head>" in low:
        idx = low.index("<head>") + 6
        return html[:idx] + JS_POLYFILLS + html[idx:]
    if "<html>" in low:
        idx = low.index("<html>") + 6
        return html[:idx] + "<head>" + JS_POLYFILLS + "</head>" + html[idx:]
    return JS_POLYFILLS + html


def vegalite_spec_to_html(spec: dict) -> str:
    schema = spec.get("$schema", "")
    vl = VEGA_LITE_V4 if "v4" in schema else VEGA_LITE_V5
    spec_json = json.dumps(spec, default=str)
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'>"
        f"{JS_POLYFILLS}"
        f"<style>body{{font-family:Arial,sans-serif;margin:10px}}"
        f"#vis{{width:100%}}#error{{color:#c00;padding:16px;display:none}}</style>"
        f"</head><body><div id='vis'></div><div id='error'></div>"
        f"<script src='{VEGA_CDN}'></script>"
        f"<script src='{vl}'></script>"
        f"<script src='{VEGA_EMBED_CDN}'></script>"
        f"<script>(function(){{try{{vegaEmbed('#vis',{spec_json},"
        f"{{actions:true,renderer:'svg'}})"
        f".catch(function(e){{var el=document.getElementById('error');"
        f"el.style.display='block';el.innerHTML='<h3>Error</h3><p>'+e.message+'</p>';}});}}"
        f"catch(e){{document.getElementById('error').textContent='JS error: '+e.message;}}}})();"
        f"</script></body></html>"
    )


def chart_to_html(chart) -> Optional[str]:
    if isinstance(chart, str):
        return inject_polyfills(chart)
    if isinstance(chart, dict):
        return vegalite_spec_to_html(chart)
    for attr in ("to_dict", "as_dict", "spec"):
        try:
            val = getattr(chart, attr)
            r = val() if callable(val) else val
            if isinstance(r, dict):
                return vegalite_spec_to_html(r)
        except Exception:
            continue
    for method in ("to_html", "_repr_html_"):
        try:
            h = getattr(chart, method)()
            if isinstance(h, str) and len(h) > 20:
                return inject_polyfills(h)
        except Exception:
            continue
    return None


def open_html_in_browser(html: str, prefix: str = "app_chart_"):
    """Write html to a temp file and open in the system browser."""
    fd, path = tempfile.mkstemp(suffix=".html", prefix=prefix)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(html)
    webbrowser.open(f"file://{os.path.abspath(path)}")


# ── safe_slot decorator ────────────────────────────────────────────────────────
def safe_slot(func):
    sig    = inspect.signature(func)
    params = [p for p in sig.parameters.values() if p.name != "self"]
    has_var = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params)
    max_pos = len([
        p for p in params
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                      inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ])

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            if has_var:
                return func(self, *args, **kwargs)
            return func(self, *args[:max_pos], **kwargs)
        except Exception as exc:
            tb = traceback.format_exc() if DEBUG else ""
            body = f"Unexpected error in <b>{func.__name__}</b>:\n\n{exc}"
            if tb:
                body += f"\n\n{tb}"
            self._show_error(body)
            try:
                self.outputText.append(f"[ERROR] {func.__name__}: {exc}")
            except Exception:
                pass
    return wrapper


# ── Splink settings builder ────────────────────────────────────────────────────
def build_splink_settings(mappings: List[Dict], dedupe_mode: bool):
    """Build a Splink SettingsCreator / dict from UI mapping rows."""
    comparisons, blocking_rules = [], []

    for m in mappings:
        col = m["left"]
        t   = m["type"]
        if SPLINK_VERSION == 4:
            if t == "Levenshtein":
                comparisons.append(cl.LevenshteinAtThresholds(col, [1, 2]))
            elif t == "Jaro-Winkler":
                comparisons.append(cl.JaroWinklerAtThresholds(col, [0.9, 0.7]))
            elif t == "Date":
                comparisons.append(cl.DatediffAtThresholds(
                    col, date_thresholds=[1, 7, 30],
                    date_metrics=["day", "day", "day"]))
            else:
                comparisons.append(cl.ExactMatch(col))
        else:
            if t == "Levenshtein":
                comparisons.append(cl.levenshtein_at_thresholds(col, [1, 2]))
            elif t == "Jaro-Winkler":
                comparisons.append(cl.jaro_winkler_at_thresholds(col, [0.9, 0.7]))
            elif t == "Date":
                comparisons.append(cl.datediff_at_thresholds(
                    col, date_thresholds=[1, 7, 30],
                    date_metrics=["day", "day", "day"]))
            else:
                comparisons.append(cl.exact_match(col))

        if m.get("block", False):
            if SPLINK_VERSION == 4:
                blocking_rules.append(block_on(col))
            else:
                blocking_rules.append(f'l."{col}" = r."{col}"')

    link_type = "dedupe_only" if dedupe_mode else "link_only"

    if SPLINK_VERSION == 4:
        return SettingsCreator(
            link_type=link_type,
            comparisons=comparisons,
            blocking_rules_to_generate_predictions=blocking_rules,
        )
    else:
        return {
            "link_type": link_type,
            "unique_id_column_name": "unique_id",
            "comparisons": comparisons,
            "blocking_rules_to_generate_predictions": blocking_rules,
        }


# ── Worker thread ──────────────────────────────────────────────────────────────
class LinkWorker(QThread):
    """Runs Splink linkage/dedupe in a background thread.

    Emits finished(dict) with keys:
        df_results, charts, linker, df_l, df_r
    When run_recon=True, also emits recon_ready(DataFrame) after the
    linkage step completes so the UI can trigger the recon worker.
    """
    progress      = pyqtSignal(str)
    finished      = pyqtSignal(object)
    recon_ready   = pyqtSignal(object)   # emitted only for AutoRecon
    error         = pyqtSignal(str)

    def __init__(self, df_left, df_right, settings, col_names,
                 threshold=0.5, max_pairs=1_000_000, dedupe=False,
                 run_recon=False):
        super().__init__()
        self.df_left    = df_left
        self.df_right   = df_right
        self.settings   = settings
        self.col_names  = col_names
        self.threshold  = threshold
        self.max_pairs  = max_pairs
        self.dedupe     = dedupe
        self.run_recon  = run_recon
        self._stop      = False

    def stop(self):
        self._stop = True

    def run(self):  # noqa: C901
        try:
            self.progress.emit("Preparing data …")
            df_l = self.df_left.copy()
            df_r = self.df_right.copy()

            for df, prefix in ((df_l, "left"), (df_r, "right")):
                if "unique_id" not in df.columns:
                    df.insert(0, "unique_id",
                              [f"{prefix}-{i}" for i in range(len(df))])
                if "source_dataset" not in df.columns:
                    df["source_dataset"] = prefix

            if self._stop:
                return

            self.progress.emit("Creating Splink linker …")
            if SPLINK_VERSION == 4:
                linker = Linker([df_l, df_r], self.settings, db_api=DuckDBAPI())
            elif SPLINK_VERSION == 3:
                linker = DuckDBLinker([df_l, df_r], self.settings)
            else:
                self.error.emit("Splink is not installed.")
                return

            if self._stop:
                return

            self.progress.emit("Step 1/3  Estimating u values …")
            if SPLINK_VERSION == 4:
                linker.training.estimate_u_using_random_sampling(
                    max_pairs=self.max_pairs)
            else:
                linker.estimate_u_using_random_sampling(
                    max_pairs=self.max_pairs)

            for idx, col in enumerate(self.col_names, 1):
                if self._stop:
                    return
                self.progress.emit(
                    f"Step 2/3  Training m ({idx}/{len(self.col_names)}): '{col}' …")
                try:
                    if SPLINK_VERSION == 4:
                        linker.training\
                            .estimate_parameters_using_expectation_maximisation(
                                block_on(col), fix_u_probabilities=True)
                    else:
                        linker.estimate_parameters_using_expectation_maximisation(
                            f'l."{col}" = r."{col}"', fix_u_probabilities=True)
                except Exception as e:
                    self.progress.emit(f"  ⚠ Could not train on '{col}': {e}")

            if self._stop:
                return

            self.progress.emit(
                f"Step 3/3  Predicting (threshold ≥ {self.threshold:.2f}) …")
            if SPLINK_VERSION == 4:
                preds = linker.inference.predict(
                    threshold_match_probability=self.threshold)
            else:
                preds = linker.predict(
                    threshold_match_probability=self.threshold)

            df_results = preds.as_pandas_dataframe()
            self.progress.emit(
                f"Found {len(df_results):,} candidate pairs "
                f"(threshold = {self.threshold:.2f}).")

            if self._stop:
                return

            charts: Dict[str, Any] = {}
            for key, method in {"match_weights": "match_weights_chart",
                                 "m_u_parameters": "m_u_parameters_chart"}.items():
                try:
                    self.progress.emit(f"Generating {key} chart …")
                    charts[key] = (getattr(linker.visualisations, method)()
                                   if SPLINK_VERSION == 4
                                   else getattr(linker, method)())
                except Exception as e:
                    self.progress.emit(f"  ⚠ {key}: {e}")

            try:
                recs = preds.as_record_dict(limit=10)
                charts["waterfall"] = (
                    linker.visualisations.waterfall_chart(recs)
                    if SPLINK_VERSION == 4
                    else linker.waterfall_chart(recs))
            except Exception as e:
                self.progress.emit(f"  ⚠ waterfall: {e}")

            result = {
                "df_results": df_results,
                "charts": charts,
                "linker": linker,
                "df_l": df_l,
                "df_r": df_r,
            }
            self.finished.emit(result)

            # For AutoRecon: signal the UI to kick off the recon worker
            if self.run_recon and not self._stop:
                self.recon_ready.emit(result)

        except Exception as e:
            self.error.emit(
                f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}")