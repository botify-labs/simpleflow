import os

from cdf.features.comparison.constants import (
    MATCHED_FILE_PATTERN,
    COMPARISON_PATH
)

# By default, when tasks pull files from s3, local files are ignored
DEFAULT_FORCE_FETCH = True

DOCS_DIRPATH = 'documents'
DOCS_NAME_PATTERN = 'url_documents.json.{}.gz'

COMPARISON_DOCS_DIRPATH = os.path.join(DOCS_DIRPATH, COMPARISON_PATH)
COMPARISON_DOCS_NAME_PATTERN = MATCHED_FILE_PATTERN

# acceptable push/index error rate limit
ERROR_RATE_LIMIT = 0.03