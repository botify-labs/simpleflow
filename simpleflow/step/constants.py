# default values for GetStepsDoneTask and MarkStepDoneTask
STEP_ACTIVITY_PARAMS_DEFAULT = {
    'schedule_to_start_timeout': 4 * 3600,
    'start_to_close_timeout': 60,
    'schedule_to_close_timeout': 4 * 3600 + 60,
    'heartbeat_timeout': 180,
    'task_priority': 100,
    'version': '1.0',
    'idempotent': True
}

UNKNOWN_CONTEXT = {
    "run_id": "unknown",
    "workflow_id": "unknown",
    "version": "unknown"
}
