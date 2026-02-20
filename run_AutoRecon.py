#!/usr/bin/env python3
"""
Entry point for AutoRecon — probabilistic matching followed by
combinatorial zero-sum reconciliation within each matched cluster.
Supports both cross-dataset (L+R) and single-dataset input modes.
"""

import app_config
app_config.APP_MODE  = "AutoRecon"
app_config.FREE_TRIAL = True

from ui_main_window import main

if __name__ == "__main__":
    main()