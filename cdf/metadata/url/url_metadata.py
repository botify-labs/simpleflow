from enum import Enum

# A intermediate definition of url data format
#
# Keys are represented in a path format
#   - ex. `metadata.h1`
#       This means `metadata` will be an object type and it
#       contains a field named `h1`
#
# Values contains
#   - type: data type of this field
#       - long: for large numeric values, such as hash value
#       - integer: for numeric values
#       - string: for string values
#       - struct: struct can contains some inner fields, but these fields
#           won't be visible when querying
#           ex. `something.redirects_from:
#               [{`id`: xx, `http_code`: xx}, {...}, ...]`
#               `redirects_from` is visible, but `redirects_from.id` is not
#           Struct's inner fields have their own types
#
#   - settings (optional): a set of setting flags of this field
#       - es:not_analyzed: this field should not be tokenized by ES
#       - es:no_index: this field should not be indexed
#       - es:multi_field: a multi_field type keeps multiple copies of the same
#           data in different format (analyzed, not_analyzed etc.)
#           In case of `multi_field`, `field_type` must be specified for
#           determine the field's type
#       - list: this field is actually a list of values in ES
#
#   - default_value (optional): the default value if this field does not
#       exist. If this key is not present, the field's default value will be
#       inferred based on its type
#       Set to `None` to avoid any default values, so if this field is missing
#       in ElasticSearch result, no default value will be added

# Type related
LONG_TYPE = 'long'
INT_TYPE = 'integer'
FLOAT_TYPE = 'float'
STRING_TYPE = 'string'
BOOLEAN_TYPE = 'boolean'
STRUCT_TYPE = 'struct'
DATE_TYPE = 'date'

# Data format related
ES_NO_INDEX = 'es:no_index'
ES_NOT_ANALYZED = 'es:not_analyzed'
ES_DOC_VALUE = 'es:doc_values'
LIST = 'list'
MULTI_FIELD = 'es:multi_field'
URL_ID = 'url_id'

# Aggregation related
# categorical fields have a finite cardinality of distinct values
AGG_CATEGORICAL = 'agg:categorical'
AGG_NUMERICAL = 'agg:numerical'

# Returns a mapping of text / nb_occurrences
# Will be notably used to store top_anchors and their number
# of occcurrences
STRING_NB_MAP_MAPPING = {
    "text": {"type": "string", "index": "not_analyzed"},
    "nb": {"type": "integer", "index": "no"},
}

# Do not push a field with this flag
# in elasticsearch mapping
FAKE_FIELD = 'es:fake'


# Comparison/Diff related
DIFF_QUALITATIVE = 'diff:qualitative'
DIFF_QUANTITATIVE = 'diff:quantitative'