#!/usr/bin/env python3
"""
Entry point for RecordLink — probabilistic record linkage between
two separate datasets.
"""

import app_config
app_config.APP_MODE  = "RecordLink"
app_config.FREE_TRIAL = True

from ui_main_window import main

if __name__ == "__main__":
    main()