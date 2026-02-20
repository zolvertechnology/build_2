"""
Central product-switching configuration.
Each launcher script sets APP_MODE before importing any other module.
"""

APP_MODE: str = "RecordLink"   # overridden by each run_*.py launcher
FREE_TRIAL: bool = True

APP_PROFILES: dict = {
    # ── RecordLink ─────────────────────────────────────────────────────────────
    "RecordLink": {
        "name_bytes":           [82, 101, 99, 111, 114, 100, 76, 105, 110, 107],
        "version":              "2.0.0",
        "support_contact":      "support@recordlink.com",
        "free_trial_row_limit": 100,
        "show_right_dataset":   True,
        "show_dedupe_checkbox": False,
        "force_dedupe_mode":    False,
        "has_recon_step":       False,
        "run_button_label":     "▶  Run Linkage",
        "status_ready":         "Ready — load two datasets to begin.",
        "data_sources_title":   "① Data Sources",
        "export_suffix":        "LinkedResults",
    },
    # ── Dedupe ─────────────────────────────────────────────────────────────────
    "Dedupe": {
        "name_bytes":           [68, 101, 100, 117, 112, 101],
        "version":              "1.0.0",
        "support_contact":      "support@dedupe.com",
        "free_trial_row_limit": 100,
        "show_right_dataset":   False,
        "show_dedupe_checkbox": False,
        "force_dedupe_mode":    True,
        "has_recon_step":       False,
        "run_button_label":     "▶  Run Deduplication",
        "status_ready":         "Ready — load your dataset to begin.",
        "data_sources_title":   "① Data Source",
        "export_suffix":        "DedupeResults",
    },
    # ── Dedupe&Link (future combined product) ──────────────────────────────────
    "DedupeLink": {
        "name_bytes":           [68, 101, 100, 117, 112, 101, 38, 76, 105, 110, 107],
        "version":              "1.0.0",
        "support_contact":      "support@dedupelink.com",
        "free_trial_row_limit": 100,
        "show_right_dataset":   True,
        "show_dedupe_checkbox": True,   # ← the option commented-out in RecordLink
        "force_dedupe_mode":    False,
        "has_recon_step":       False,
        "run_button_label":     "▶  Run",
        "status_ready":         "Ready — load dataset(s) to begin.",
        "data_sources_title":   "① Data Sources",
        "export_suffix":        "Results",
    },
    # ── AutoRecon ──────────────────────────────────────────────────────────────
    "AutoRecon": {
        "name_bytes":           [65, 117, 116, 111, 82, 101, 99, 111, 110],
        "version":              "1.0.0",
        "support_contact":      "support@autorecon.com",
        "free_trial_row_limit": 100,
        "show_right_dataset":   True,
        "show_dedupe_checkbox": True,
        "force_dedupe_mode":    False,
        "has_recon_step":       True,
        "run_button_label":     "▶  Run AutoRecon",
        "status_ready":         "Ready — load dataset(s) to begin.",
        "data_sources_title":   "① Data Sources",
        "export_suffix":        "ReconResults",
    },
    # ── SumMatch ───────────────────────────────────────────────────────────────
    "SumMatch": {
        "name_bytes":                   [83, 117, 109, 77, 97, 116, 99, 104],
        "version":                      "1.4.0",
        "support_contact":              "support@summatch.com",
        "free_trial_row_limit":         50,
        # SumMatch has its own dedicated window (ui_summatch_window.py);
        # the flags below are kept for completeness but are not read by
        # ui_main_window.py (which is never loaded for this product).
        "show_right_dataset":           False,
        "show_dedupe_checkbox":         False,
        "force_dedupe_mode":            False,
        "has_recon_step":               False,
        # SumMatch-specific advanced controls (hidden from users by default)
        "show_static_ordering":         False,
        "show_subset_generation_mode":  False,
        "default_static_ordering":      False,
        "default_subset_generation_mode": "Original Mode",
        # Labels / strings
        "run_button_label":             "Start Solving",
        "status_ready":                 "Ready — load a dataset to begin.",
        "data_sources_title":           "① Data Source",
        "export_suffix":                "SumMatchResults",
    },
}


def get_profile() -> dict:
    return APP_PROFILES[APP_MODE]


def get_app_name() -> str:
    return "".join(chr(b) for b in get_profile()["name_bytes"])