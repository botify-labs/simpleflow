import logging


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

LOGGING = {
    'version': 1,
    'loggers': {
        'simpleflow': {
            'level': 'INFO',
            'handlers': ['console'],
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'stream': 'ext://sys.stderr',
        },
    },
}
