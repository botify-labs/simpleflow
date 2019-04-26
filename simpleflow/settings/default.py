import logging  # NOQA
import os


WORKFLOW_DEFAULT_TASK_LIST = 'default'
WORKFLOW_DEFAULT_VERSION = 'default'
WORKFLOW_DEFAULT_EXECUTION_TIME = str(60 * 60)  # 1 hour.
WORKFLOW_DEFAULT_DECISION_TASK_TIMEOUT = str(5 * 60)  # 5 minutes.

ACTIVITY_DEFAULT_TASK_LIST = 'default'
ACTIVITY_DEFAULT_VERSION = 'default'
ACTIVITY_DEFAULT_TIMEOUT = str(5 * 60)  # 5 minutes.
ACTIVITY_START_TO_CLOSE_TIMEOUT = ACTIVITY_DEFAULT_TIMEOUT
ACTIVITY_SCHEDULE_TO_CLOSE_TIMEOUT = ACTIVITY_DEFAULT_TIMEOUT
ACTIVITY_SCHEDULE_TO_START_TIMEOUT = ACTIVITY_DEFAULT_TIMEOUT
ACTIVITY_HEARTBEAT_TIMEOUT = ACTIVITY_DEFAULT_TIMEOUT

SIMPLEFLOW_S3_HOST = 's3.amazonaws.com'
SIMPLEFLOW_S3_SSE = False

STEP_BUCKET = 'step_bucket'

METROLOGY_BUCKET = 'metrology_bucket'
METROLOGY_PATH_PREFIX = None

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        'simpleflow': {
            'level': os.getenv('LOG_LEVEL', 'INFO').upper(),
            'handlers': ['console'],
            # avoid duplicate logs if loggers up are defined (e.g. root)
            # + higher-level loggers tend to have worse/lame formatters
            'propagate': False,
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'stream': 'ext://sys.stderr',
            'formatter': 'simpleflow_formatter',
        },
    },
    'formatters': {
        'simpleflow_formatter': {
            '()': 'simpleflow.log.SimpleflowFormatter',
            'format': '%(asctime)s %(message)s',
        },
    }
}
SIMPLEFLOW_SYSLOG_TARGET = None

SIMPLEFLOW_ENABLE_DISK_CACHE = False
SIMPLEFLOW_BINARIES_DIRECTORY = '/tmp/simpleflow-binaries'

# Activity management

# Amount of time to wait for process spawned by an activity poller to wait in
# response to a SIGTERM.
ACTIVITY_SIGTERM_WAIT_SEC = 3
