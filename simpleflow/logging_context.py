import os


ENV_KEYS = {
    "activity_id": "_SWF_CONTEXT_ACTIVITY_ID",
    "domain": "_SWF_CONTEXT_DOMAIN",
    "event_id": "_SWF_CONTEXT_EVENT_ID",
    "task_list": "_SWF_CONTEXT_TASK_LIST",
    "task_type": "_SWF_CONTEXT_TASK_TYPE",
    "workflow_id": "_SWF_CONTEXT_WORKFLOW_ID",
}


def set(key, value):
    env_var = ENV_KEYS[key]
    os.environ[env_var] = str(value)


def get(key):
    env_var = ENV_KEYS[key]
    return os.getenv(env_var, "")


def reset():
    for env_var in ENV_KEYS.values():
        os.environ[env_var] = ""
