import sys

from . import default


class Setting(object):
    pass


def is_definition(var):
    if var.startswith('_'):
        return False
    return all(c.isupper() for c in var if c.isalpha())


def get_settings(module):
    return {
        var: getattr(module, var) for var in dir(module) if
        is_definition(var)
    }


def load_settings(module, env, conf, defaults):
    settings = {}
    value = None
    for name, typ in get_settings(module).iteritems():
        if name in env:
            value = env[name]
        elif name in conf:
            value = conf[name]
        else:
            value = getattr(defaults, name)

        settings[name] = typ(value)

    return settings


def load(conf_module_name=None):
    import os

    env = os.environ
    conf_module_name = conf_module_name or env.get('SIMPLEFLOW_SETTINGS_MODULE')
    if conf_module_name:
        conf = __import__(conf_module_name, fromlist=['*'])
    else:
        conf = {}

    return load_settings(
        sys.modules[__name__],
        env,
        conf,
        default,
    )


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
