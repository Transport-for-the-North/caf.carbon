"""
Specify input and output folder locations
"""
# Built-Ins
from pathlib import Path

# fleet emissions tool data
REGION_FILTER = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "msoa_region.csv"

# vkm emissions tool data
BATCHED_PROCESSING_FILE = \
    Path.cwd() / \
    "Model input data" / "batch_run_set_up.csv"