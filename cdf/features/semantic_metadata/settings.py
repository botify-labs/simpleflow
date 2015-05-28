from enum import Enum


NAME = "Semantic metadata"
DESCRIPTION = "title, description, h1, h2 and h3 values"
ORDER = 50

CONTENT_TYPE_INDEX = {
    1: 'title',
    2: 'h1',
    3: 'h2',
    4: 'description',
    5: 'h3'
}
CONTENT_TYPE_NAME_TO_ID = {v: k for k, v in CONTENT_TYPE_INDEX.iteritems()}

MANDATORY_CONTENT_TYPES = ('title', 'h1', 'description')
MANDATORY_CONTENT_TYPES_IDS = (1, 2, 4)

GROUPS = Enum('Groups', [('semantic_metadata', 'HTML Tags')])
