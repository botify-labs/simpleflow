from enum import Enum
from cdf.metadata.url.url_metadata import BOOLEAN_TYPE

# Separator for encoding the url_id with the url string
# 2 conditions:
#   - not a valid char in url string
#   - does not alter the url string ordering
# with above in mind, `\0` seems to be the best choice
SEPARATOR = '\0'


# File and directory constants
MATCHED_FILE_PATTERN = 'matched.json.{}.gz'
MATCHED_FILE_REGEXP = MATCHED_FILE_PATTERN.format('[0-9]+')
COMPARISON_PATH = 'comparison'


class MatchingState(Enum):
    MATCH = 1
    DISCOVER = 2
    DISAPPEAR = 3


# The document merge hack needs some extra flag fields
# Plus to this, we'll need to have a `previous` field
# which is a hard copy of the actual mapping
EXTRA_FIELDS_FORMAT = {
    'disappeared': {
        'type': BOOLEAN_TYPE,
        'default_value': None
    },
    'previous_exists': {
        'type': BOOLEAN_TYPE,
        'default_value': None
    }
}