from enum import Enum


NAME = "Data Extraction"
DESCRIPTION = "Retrieve data extracted from the HTML pages"
ORDER = 100

NB_TOP_ANCHORS = 5

GROUPS = Enum(
    'Groups',
    [
        ("extract", "Extracted Data")
    ]
)
