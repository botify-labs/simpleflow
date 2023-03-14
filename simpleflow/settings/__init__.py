from __future__ import annotations

import sys
from pprint import pformat
from typing import TYPE_CHECKING

from . import base

if TYPE_CHECKING:
    from typing import Any


def put_setting(key: str, value: Any):
    setattr(sys.modules[__name__], key, value)
    _keys.add(key)


def configure(dct: dict) -> None:
    for k, v in dct.items():
        put_setting(k, v)


def print_settings():
    for key in sorted(_keys):
        value = getattr(sys.modules[__name__], key)
        print(f"{key}={pformat(value)}")


# initialize a list of settings names
_keys: set[str] = set()

# look for settings and initialize them
configure(base.load())

# Typing
WORKFLOW_DEFAULT_TASK_LIST: str
WORKFLOW_DEFAULT_VERSION: str
WORKFLOW_DEFAULT_EXECUTION_TIME: str
WORKFLOW_DEFAULT_DECISION_TASK_TIMEOUT: str

ACTIVITY_DEFAULT_TASK_LIST: str
ACTIVITY_DEFAULT_VERSION: str
ACTIVITY_DEFAULT_TIMEOUT: str
ACTIVITY_START_TO_CLOSE_TIMEOUT: str
ACTIVITY_SCHEDULE_TO_CLOSE_TIMEOUT: str
ACTIVITY_SCHEDULE_TO_START_TIMEOUT: str
ACTIVITY_HEARTBEAT_TIMEOUT: str

SIMPLEFLOW_S3_HOST: str
SIMPLEFLOW_S3_SSE: bool

STEP_BUCKET: str

METROLOGY_BUCKET: str
METROLOGY_PATH_PREFIX: str | None

LOGGING: dict[str, Any]
SIMPLEFLOW_SYSLOG_TARGET: str | None

SIMPLEFLOW_ENABLE_DISK_CACHE: bool
SIMPLEFLOW_BINARIES_DIRECTORY: str

# Activity management

# Amount of time to wait for process spawned by an activity poller to wait in
# response to a SIGTERM.
ACTIVITY_SIGTERM_WAIT_SEC: int
