#!/usr/bin/env python3
"""
Entry point for SumMatch — combinatorial sum-matching within
a single dataset.
"""

import app_config
app_config.APP_MODE   = "SumMatch"
app_config.FREE_TRIAL = True

from ui_summatch_window import main

if __name__ == "__main__":
    main()