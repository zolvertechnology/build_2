"""
Combinatorial zero-sum reconciliation engine.
Contains: CandidateRuleEngine, ReconWorker, StandaloneSearchWorker,
          LazyTableModel.
"""

import math
import threading
import time
import traceback
import itertools
import concurrent.futures
from typing import Optional

import numpy as np
import pandas as pd

from qt_compat import (
    PYQT, QThread, QObject, pyqtSignal,
    QAbstractTableModel, QModelIndex,
    _DisplayRole, _Horizontal,
)

DEBUG = False


# ── LazyTableModel ─────────────────────────────────────────────────────────────
class LazyTableModel(QAbstractTableModel):
    """Incrementally loads rows as the user scrolls."""

    def __init__(self, df: pd.DataFrame, parent=None):
        super().__init__(parent)
        self._df      = df
        self._loaded  = min(100, df.shape[0])
        self._inc     = 50

    def rowCount(self, parent=QModelIndex()):    return self._loaded
    def columnCount(self, parent=QModelIndex()): return self._df.shape[1] if self._df is not None else 0

    def data(self, index, role=_DisplayRole):
        if not index.isValid() or role != _DisplayRole:
            return None
        r, c = index.row(), index.column()
        if r >= self._df.shape[0] or c >= self._df.shape[1]:
            return None
        return str(self._df.iloc[r, c])

    def headerData(self, section, orientation, role=_DisplayRole):
        if role != _DisplayRole:
            return None
        if orientation == _Horizontal:
            return (str(self._df.columns[section])
                    if 0 <= section < self._df.shape[1] else None)
        return str(section)

    def loadMore(self):
        if self._loaded < self._df.shape[0]:
            new = min(self._inc, self._df.shape[0] - self._loaded)
            self.beginInsertRows(QModelIndex(), self._loaded,
                                 self._loaded + new - 1)
            self._loaded += new
            self.endInsertRows()


# ── CandidateRuleEngine ────────────────────────────────────────────────────────
class CandidateRuleEngine:
    """Branch-and-bound BFS to find row subsets whose amounts sum to target."""

    def __init__(self, df, candidate_cols, amount_col, target, tol_value,
                 static_ordering=False, subset_mode="Original Mode"):
        self.df                     = df.copy()
        self.candidate_cols         = candidate_cols
        self.amount_col             = amount_col
        self.target                 = target
        self.tol                    = float(tol_value)
        self.static_ordering        = static_ordering
        self.subset_generation_mode = (
            "new" if subset_mode == "New Mode" else "original")

        self.int_cat_arrays           = {}
        self.cat_masks                = {}
        self.all_categories           = {}
        self.int_to_cat               = {}
        self.all_columns              = candidate_cols[:]
        self.value_array              = None
        self.TARGET_SUM               = None
        self.cancel_search            = False   # set to True to abort
        self.candidate_rule_results   = []
        self.memoization_cache        = {}
        self.memo_lock                = threading.Lock()
        self.progress_callback        = None
        self.candidate_search_start_time = None
        self.solution_mask_map        = {}
        self.solution_counter         = 0
        self.kept_indices             = None
        self.zero_rows_excluded       = 0

    # ── internal helpers ────────────────────────────────────────────────────
    def _amount_decimal_places(self):
        if self.tol <= 0:
            return 2
        dp = -int(math.floor(math.log10(self.tol))) if self.tol > 0 else 2
        return max(2, dp)

    def _log(self, msg):
        if self.progress_callback:
            self.progress_callback(msg)

    def set_progress_callback(self, cb):
        self.progress_callback = cb

    def rule_to_key(self, rule):
        return tuple(
            (col, tuple(sorted(rule[col])))
            for col in self.all_columns if col in rule
        )

    def rule_to_real(self, rule):
        return {
            col: {self.int_to_cat[col].get(c, c) for c in cats}
            for col, cats in rule.items()
            if col in self.all_columns
        }

    def mirror_rule(self, rule):
        if not rule:
            return {}
        last_col = list(rule.keys())[-1]
        return {
            col: (self.all_categories[col] - vals
                  if col == last_col else vals)
            for col, vals in rule.items()
        }

    # ── data preparation ────────────────────────────────────────────────────
    def prepare_data(self):
        self.TARGET_SUM = float(self.target)
        self.tol        = float(self.tol)

        non_zero = self.df[self.amount_col].astype(float) != 0
        self.zero_rows_excluded = int(np.sum(~non_zero.values))
        self.kept_indices       = np.where(non_zero.values)[0]
        self.df = self.df[non_zero]

        for col in self.candidate_cols:
            data        = self.df[col].fillna("_Blank_")
            unique_vals = sorted(data.unique(), key=str)
            self.all_categories[col] = set(range(len(unique_vals)))
            c2i = {v: i for i, v in enumerate(unique_vals)}
            self.int_to_cat[col]     = {i: v for i, v in enumerate(unique_vals)}
            self.int_cat_arrays[col] = np.array(data.map(c2i))
            self.cat_masks[col]      = {
                i: (self.int_cat_arrays[col] == i)
                for i in range(len(unique_vals))
            }

        self.value_array = np.array(self.df[self.amount_col].astype(float))

    # ── pruning ─────────────────────────────────────────────────────────────
    def check_pruning(self, mask, current_sum):
        if current_sum < self.TARGET_SUM:
            ub = np.sum(self.value_array[mask & (self.value_array >= 0)])
            if ub < self.TARGET_SUM - self.tol:
                return False
        elif current_sum > self.TARGET_SUM:
            lb = np.sum(self.value_array[mask & (self.value_array <= 0)])
            if lb > self.TARGET_SUM + self.tol:
                return False
        return True

    # ── subset generators ───────────────────────────────────────────────────
    def generate_candidate_subsets_pruned_original(self, col, avail, current_mask):
        avail        = set(avail)
        global_total = np.sum(self.value_array)

        def get_mask(candidate):
            return current_mask & np.logical_or.reduce(
                [self.cat_masks[col][c] for c in candidate])

        candidate = set(avail)
        new_mask  = get_mask(candidate)
        new_sum   = np.sum(self.value_array[new_mask])
        if self.check_pruning(new_mask, new_sum):
            yield candidate, new_mask, new_sum

        mirror     = avail - candidate
        mirror_sum = global_total - new_sum
        if (mirror and self.check_pruning(new_mask, mirror_sum)
                and abs(mirror_sum - self.TARGET_SUM) <= self.tol):
            yield mirror, get_mask(mirror), mirror_sum

        def rec(curr, start):
            for i in range(start, len(sorted(curr))):
                nc = curr.copy()
                nc.discard(sorted(curr)[i])
                if not nc:
                    continue
                nm  = get_mask(nc)
                ns  = np.sum(self.value_array[nm])
                if ns < self.TARGET_SUM:
                    ub = np.sum(self.value_array[nm & (self.value_array >= 0)])
                    if ub < self.TARGET_SUM - self.tol:
                        continue
                elif ns > self.TARGET_SUM:
                    lb = np.sum(self.value_array[nm & (self.value_array <= 0)])
                    if lb > self.TARGET_SUM + self.tol:
                        continue
                if self.check_pruning(nm, ns):
                    yield nc, nm, ns
                mr   = avail - nc
                msum = global_total - ns
                if (mr and self.check_pruning(nm, msum)
                        and abs(msum - self.TARGET_SUM) <= self.tol):
                    yield mr, get_mask(mr), msum
                yield from rec(nc, i)

        yield from rec(candidate, 0)

    def generate_candidate_subsets_pruned_new(self, col, avail, current_mask):
        avail_list   = sorted(avail)
        avail_set    = set(avail_list)
        global_total = np.sum(self.value_array)
        pruned       = []

        for r in range(len(avail_list), 0, -1):
            for comb in itertools.combinations(avail_list, r):
                candidate = set(comb)
                if any(candidate.issubset(p) for p in pruned):
                    continue
                nm = current_mask & np.logical_or.reduce(
                    [self.cat_masks[col][c] for c in candidate])
                ns     = np.sum(self.value_array[nm])
                mirror = avail_set - candidate

                if ns < self.TARGET_SUM:
                    ub = np.sum(self.value_array[nm & (self.value_array >= 0)])
                    if ub < self.TARGET_SUM - self.tol:
                        pruned.append(candidate)
                        continue
                elif ns > self.TARGET_SUM:
                    lb = np.sum(self.value_array[nm & (self.value_array <= 0)])
                    if lb > self.TARGET_SUM + self.tol:
                        pruned.append(candidate)
                        continue

                if self.check_pruning(nm, ns):
                    yield candidate, nm, ns
                msum = global_total - ns
                if (mirror and self.check_pruning(nm, msum)
                        and abs(msum - self.TARGET_SUM) <= self.tol):
                    mirror_mask = current_mask & np.logical_or.reduce(
                        [self.cat_masks[col][c] for c in mirror])
                    yield mirror, mirror_mask, msum

    # ── solution registration ────────────────────────────────────────────────
    def complete_rule(self, partial_rule, current_mask):
        comp = partial_rule.copy()
        for col in self.all_columns:
            if col not in comp:
                comp[col] = set(
                    np.unique(self.int_cat_arrays[col][current_mask]))
        return comp

    def register_solution(self, complete, original_rule,
                          current_mask, current_sum, prefix):
        full_mask = np.ones(self.value_array.shape[0], dtype=bool)
        for col, cats in complete.items():
            if col not in self.all_columns:
                continue
            full_mask &= np.logical_or.reduce(
                [self.cat_masks[col][c] for c in cats])
        mask_key = tuple(np.where(full_mask)[0])
        dp = self._amount_decimal_places()

        with self.memo_lock:
            if mask_key in self.solution_mask_map:
                return False
            self.solution_counter += 1
            sid = self.solution_counter
            self.solution_mask_map[mask_key] = sid
            complete["_solution_id"] = sid
            elapsed = (time.time() - self.candidate_search_start_time
                       if self.candidate_search_start_time else 0)
            msg = (f"[{elapsed:.2f}s] Solution {sid} found: "
                   f"Sum Amount: {current_sum:.{dp}f}")
            print(msg)
            self._log(msg)
            self.candidate_rule_results.append(complete)
        return True

    # ── BFS search ──────────────────────────────────────────────────────────
    def process_state(self, state):
        if self.cancel_search:
            return []

        current_rule = state["rule"]
        current_mask = state["mask"]
        current_sum  = np.sum(self.value_array[current_mask])

        mirror     = self.mirror_rule(current_rule)
        rule_key   = self.rule_to_key(current_rule)
        mirror_key = self.rule_to_key(mirror)
        dup = False
        with self.memo_lock:
            if (rule_key in self.memoization_cache
                    or mirror_key in self.memoization_cache):
                dup = True
            else:
                self.memoization_cache[rule_key]   = True
                self.memoization_cache[mirror_key] = True

        if dup:
            if abs(current_sum - self.TARGET_SUM) <= self.tol:
                comp = self.complete_rule(current_rule, current_mask)
                self.register_solution(comp, current_rule,
                                       current_mask, current_sum, "")
            return []

        if not self.check_pruning(current_mask, current_sum):
            return []

        remaining = [c for c in self.all_columns if c not in current_rule]
        if not remaining:
            if abs(current_sum - self.TARGET_SUM) <= self.tol:
                comp = self.complete_rule(current_rule, current_mask)
                self.register_solution(comp, current_rule,
                                       current_mask, current_sum, "")
            return []

        def avail(col):
            return set(np.unique(self.int_cat_arrays[col][current_mask]))

        next_col = (
            next(c for c in self.all_columns if c not in current_rule)
            if self.static_ordering
            else min(remaining, key=lambda c: len(avail(c)))
        )

        av = avail(next_col)
        if not av:
            return []

        gen = (
            self.generate_candidate_subsets_pruned_new(
                next_col, av, current_mask)
            if self.subset_generation_mode == "new"
            else self.generate_candidate_subsets_pruned_original(
                next_col, av, current_mask)
        )

        next_states = []
        for subset, new_mask, new_sum in gen:
            if self.cancel_search:
                return []
            if not self.check_pruning(new_mask, new_sum):
                continue
            new_rule = {**current_rule, next_col: subset}
            if abs(new_sum - self.TARGET_SUM) <= self.tol:
                comp = self.complete_rule(new_rule, new_mask)
                self.register_solution(comp, new_rule,
                                       new_mask, new_sum, "")
            next_states.append({"rule": new_rule, "mask": new_mask})
        return next_states

    def parallel_bfs_search_rule_dynamic(self):
        current_level = [
            {"rule": {}, "mask": np.ones(len(self.value_array), dtype=bool)}
        ]
        while current_level and not self.cancel_search:
            next_level = []
            with concurrent.futures.ThreadPoolExecutor() as ex:
                futures = [ex.submit(self.process_state, s)
                           for s in current_level]
                for fut in futures:
                    if self.cancel_search:
                        break
                    res = fut.result()
                    if isinstance(res, list):
                        next_level.extend(res)
            next_level.sort(key=lambda s: self.rule_to_key(s["rule"]))
            current_level = next_level

    def search(self):
        self.candidate_rule_results.clear()
        self.memoization_cache.clear()
        self.solution_mask_map.clear()
        self.solution_counter = 0
        self.candidate_search_start_time = time.time()
        self.parallel_bfs_search_rule_dynamic()

    def get_candidate_results(self):
        seen = {}
        for rule in self.candidate_rule_results:
            seen[self.rule_to_key(rule)] = rule
        return list(seen.values())


# ── Union-Find for cluster assignment ──────────────────────────────────────────
def compute_clusters_from_pairs(df_pairs: pd.DataFrame) -> dict:
    """Return {unique_id: cluster_id} using Union-Find on Splink pair output."""
    parent: dict = {}

    def find(x):
        if x not in parent:
            parent[x] = x
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    uid_l = ("unique_id_l" if "unique_id_l" in df_pairs.columns
             else df_pairs.columns[0])
    uid_r = ("unique_id_r" if "unique_id_r" in df_pairs.columns
             else df_pairs.columns[1])

    for uid in df_pairs[uid_l].tolist() + df_pairs[uid_r].tolist():
        if str(uid) not in parent:
            parent[str(uid)] = str(uid)

    for _, row in df_pairs.iterrows():
        union(str(row[uid_l]), str(row[uid_r]))

    root_to_id: dict = {}
    counter = [0]
    result:  dict = {}
    for uid in list(parent.keys()):
        root = find(uid)
        if root not in root_to_id:
            counter[0] += 1
            root_to_id[root] = counter[0]
        result[uid] = root_to_id[root]
    return result


# ── ReconWorker — runs AFTER Splink, per-cluster zero-sum search ───────────────
class ReconWorker(QThread):
    """Assigns cluster IDs from Splink pairs then finds zero-sum subsets
    within each cluster using CandidateRuleEngine."""

    progress = pyqtSignal(str)
    finished = pyqtSignal(object)
    error    = pyqtSignal(str)

    def __init__(self, df_results: pd.DataFrame,
                 df_l: pd.DataFrame, df_r: pd.DataFrame,
                 amount_col: str, mapped_cols: list,
                 recon_tol: float = 0.01):
        super().__init__()
        self.df_results  = df_results
        self.df_l        = df_l
        self.df_r        = df_r
        self.amount_col  = amount_col
        self.mapped_cols = mapped_cols
        self.recon_tol   = recon_tol
        self._stop       = False

    def stop(self):
        self._stop = True

    def run(self):
        try:
            self.progress.emit(
                "AutoRecon: computing clusters from Splink results …")
            cluster_map = compute_clusters_from_pairs(self.df_results)

            df_combined = pd.concat(
                [self.df_l, self.df_r], ignore_index=True)
            df_combined["cluster_id"] = df_combined["unique_id"].map(
                cluster_map)
            df_combined = df_combined.dropna(subset=["cluster_id"])
            df_combined["cluster_id"] = \
                df_combined["cluster_id"].astype(int)

            if self.amount_col not in df_combined.columns:
                self.error.emit(
                    f"Amount column '{self.amount_col}' not found in "
                    f"combined dataset.")
                return

            n_clusters = df_combined["cluster_id"].nunique()
            self.progress.emit(
                f"AutoRecon: {n_clusters} cluster(s) found — "
                f"searching for zero-sum subsets …")

            grouping_cols = [
                c for c in self.mapped_cols
                if c in df_combined.columns and c != self.amount_col
            ]
            if not grouping_cols:
                grouping_cols = [
                    c for c in df_combined.columns
                    if c not in ("unique_id", "source_dataset",
                                 "cluster_id", self.amount_col)
                ]

            df_combined = df_combined.copy()
            df_combined["recon_group"] = ""
            global_group_counter = 0

            for cid, grp in df_combined.groupby("cluster_id"):
                if self._stop:
                    break
                if len(grp) < 2:
                    continue
                sub = grp.reset_index(drop=True)
                try:
                    engine = CandidateRuleEngine(
                        sub, grouping_cols, self.amount_col,
                        target=0, tol_value=self.recon_tol)
                    engine.prepare_data()
                    engine.search()
                    rules = engine.get_candidate_results()
                except Exception as e:
                    self.progress.emit(f"  ⚠ Cluster {cid}: {e}")
                    continue

                if not rules:
                    continue

                for rule in rules:
                    mask = np.ones(len(engine.value_array), dtype=bool)
                    for col, cats in rule.items():
                        if col not in engine.all_columns:
                            continue
                        mask &= np.logical_or.reduce(
                            [engine.cat_masks[col][c] for c in cats])
                    fi = np.where(mask)[0]
                    local_idx = (engine.kept_indices[fi]
                                 if engine.kept_indices is not None
                                 else fi)
                    global_group_counter += 1
                    label = str(global_group_counter)
                    for pos in local_idx:
                        orig_idx = grp.index[pos]
                        existing = df_combined.at[orig_idx, "recon_group"]
                        if existing:
                            parts = sorted(
                                set(existing.split("_")) | {label},
                                key=lambda x: int(x))
                            df_combined.at[orig_idx, "recon_group"] = \
                                "_".join(parts)
                        else:
                            df_combined.at[orig_idx, "recon_group"] = label

                self.progress.emit(
                    f"  Cluster {cid}: {len(rules)} zero-sum group(s) found.")

            total = (df_combined["recon_group"] != "").sum()
            self.progress.emit(
                f"AutoRecon complete — {global_group_counter} "
                f"zero-sum group(s), {total:,} reconciled row(s).")
            self.finished.emit(df_combined)

        except Exception as e:
            self.error.emit(
                f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}")


# ── StandaloneSearchWorker ─────────────────────────────────────────────────────
class StandaloneSearchWorker(QObject):
    """Wraps CandidateRuleEngine for use inside a QThread (moveToThread pattern).

    Used by both SumMatch (direct standalone search) and AutoRecon
    (single-dataset recon mode).

    Cancellation is two-stage:
      1.  self._cancel  — checked before engine.search() starts
      2.  engine.cancel_search — set immediately via cancel() so the BFS
          loop aborts on the next state-processing iteration.
    """

    finished = pyqtSignal(str, object)
    progress = pyqtSignal(str)

    def __init__(self, df, candidate_cols, amount_col, target, tol_value,
                 static_ordering=False, subset_mode="Original Mode",
                 parent=None):
        super().__init__(parent)
        self.df             = df
        self.candidate_cols = candidate_cols[:]
        self.amount_col     = amount_col
        self.target         = target
        self.tol_value      = tol_value
        self.static_ordering = static_ordering
        self.subset_mode    = subset_mode
        self._cancel        = False
        self._engine: Optional[CandidateRuleEngine] = None

    def cancel(self):
        """Call from the main thread to abort the running search immediately."""
        self._cancel = True
        eng = self._engine          # read once (avoids race with assignment)
        if eng is not None:
            eng.cancel_search = True

    def run_search(self):
        try:
            start  = time.time()
            engine = CandidateRuleEngine(
                self.df, self.candidate_cols, self.amount_col,
                self.target, self.tol_value,
                self.static_ordering, self.subset_mode)

            # Store reference BEFORE search so cancel() can reach it
            self._engine = engine
            # Propagate any cancellation that arrived before engine was ready
            if self._cancel:
                engine.cancel_search = True

            engine.prepare_data()
            engine.set_progress_callback(self.progress.emit)

            if engine.zero_rows_excluded:
                self.progress.emit(
                    f"Note: {engine.zero_rows_excluded} zero-amount row(s) "
                    f"excluded (they cannot affect sums).")

            engine.search()

            elapsed = time.time() - start
            rules   = engine.get_candidate_results()

            if rules:
                original_row_count = self.df.shape[0]
                sol_col = [""] * original_row_count

                for idx, rule in enumerate(rules, 1):
                    mask = np.ones(len(engine.value_array), dtype=bool)
                    for col, cats in rule.items():
                        if col not in engine.all_columns:
                            continue
                        mask &= np.logical_or.reduce(
                            [engine.cat_masks[col][c] for c in cats])
                    fi = np.where(mask)[0]
                    oi = (engine.kept_indices[fi]
                          if engine.kept_indices is not None else fi)
                    for i in oi:
                        ex = sol_col[i]
                        if ex:
                            parts = sorted(
                                set(ex.split("_")) | {str(idx)},
                                key=lambda x: int(x))
                            sol_col[i] = "_".join(parts)
                        else:
                            sol_col[i] = str(idx)

                out_df = self.df.copy()
                out_df["solution_set"] = sol_col
                msg = (f"Time spent: {elapsed:.4f}s\n"
                       f"Solutions found: {len(rules)}\n")
            else:
                out_df = None
                msg    = (f"Time spent: {elapsed:.4f}s\n"
                          f"No solution was found.\n")

            self.finished.emit(msg, out_df)

        except ValueError as e:
            self.finished.emit(f"Input error: {e}", None)
        except Exception as e:
            tb = traceback.format_exc()
            self.finished.emit(
                f"An unexpected error occurred: {e}\n\n{tb}", None)