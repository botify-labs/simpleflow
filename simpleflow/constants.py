import os

SIMPLEFLOW_ENV = os.getenv("SIMPLEFLOW_ENV", "production")

# Constants useful for defining SWF timeouts
MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR

# Formatting limits
MAX_REASON_LENGTH = 256
MAX_DETAILS_LENGTH = 32768
MAX_EXECUTION_CONTEXT_LENGTH = 32768
MAX_HEARTBEAT_DETAILS_LENGTH = 2048
MAX_IDENTITY_LENGTH = 256
# TODO: experiment with the 2 following limits:
# - the general limits doc states they're 32000: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dg-limits.html  # noqa
# - the API docs state they're 32768: http://docs.aws.amazon.com/amazonswf/latest/apireference/API_StartWorkflowExecution.html etc.  # noqa
MAX_INPUT_LENGTH = 32000
MAX_RESULT_LENGTH = 32000

MAX_LOG_FIELD = 500 * 1024  # 500kB

# Jumbo fields
JUMBO_FIELDS_PREFIX = "simpleflow+s3://"
JUMBO_FIELDS_MAX_SIZE = 5 * 1024 ** 2  # 5MB

# Cache directory
CACHE_DIR = "/tmp/simpleflow-cache"
