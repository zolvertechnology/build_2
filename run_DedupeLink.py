#!/usr/bin/env python3
"""
Entry point for Dedupe&Link — combined single-table deduplication
AND cross-table linkage in one tool.
Development mode: kept separate from RecordLink and Dedupe until
the product strategy is confirmed.
"""

import app_config
app_config.APP_MODE  = "DedupeLink"
app_config.FREE_TRIAL = True

from ui_main_window import main

if __name__ == "__main__":
    main()