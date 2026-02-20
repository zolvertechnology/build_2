#!/usr/bin/env python3
"""
Entry point for Dedupe — deduplication within a single dataset.
"""

import app_config
app_config.APP_MODE  = "Dedupe"
app_config.FREE_TRIAL = True

from ui_main_window import main

if __name__ == "__main__":
    main()