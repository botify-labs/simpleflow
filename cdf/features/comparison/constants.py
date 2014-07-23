from enum import Enum

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