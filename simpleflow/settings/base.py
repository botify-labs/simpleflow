from __future__ import annotations

import sys

from . import default


class Setting:
    pass


def is_definition(var):
    if var.startswith("_"):
        return False
    return all(c.isupper() for c in var if c.isalpha())


def get_settings(module):
    return {var: getattr(module, var) for var in dir(module) if is_definition(var)}


def load_settings(module, env, conf, defaults):
    settings = {}
    for name, typ in get_settings(module).items():
        if name in env:
            value = env[name]
        elif conf and hasattr(conf, name):
            value = getattr(conf, name)
        else:
            value = getattr(defaults, name)

        settings[name] = typ(value)

    return settings


def load(conf_module_name=None):
    import os

    env = os.environ
    conf_module_name = conf_module_name or env.get("SIMPLEFLOW_SETTINGS_MODULE")
    if conf_module_name:
        conf = __import__(conf_module_name, fromlist=["*"])
    else:
        conf = None

    return load_settings(
        sys.modules[__name__],
        env,
        conf,
        default,
    )


def str_or_none(val):
    return val or None


WORKFLOW_DEFAULT_TASK_LIST = str
WORKFLOW_DEFAULT_VERSION = str
WORKFLOW_DEFAULT_EXECUTION_TIME = str
WORKFLOW_DEFAULT_DECISION_TASK_TIMEOUT = str

ACTIVITY_DEFAULT_TASK_LIST = str
ACTIVITY_DEFAULT_VERSION = str
ACTIVITY_DEFAULT_TIMEOUT = str
ACTIVITY_START_TO_CLOSE_TIMEOUT = str
ACTIVITY_SCHEDULE_TO_CLOSE_TIMEOUT = str
ACTIVITY_SCHEDULE_TO_START_TIMEOUT = str
ACTIVITY_HEARTBEAT_TIMEOUT = str

LOGGING = dict
SIMPLEFLOW_SYSLOG_TARGET = str_or_none

SIMPLEFLOW_S3_HOST = str
SIMPLEFLOW_S3_SSE = bool

STEP_BUCKET = str

METROLOGY_BUCKET = str
METROLOGY_PATH_PREFIX = str_or_none

SIMPLEFLOW_ENABLE_DISK_CACHE = bool
SIMPLEFLOW_BINARIES_DIRECTORY = str

ACTIVITY_SIGTERM_WAIT_SEC = float
