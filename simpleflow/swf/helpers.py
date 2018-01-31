from __future__ import absolute_import

import getpass
import json
import os
import socket

from future.utils import iteritems

import swf.exceptions
import swf.models
import swf.querysets
from simpleflow.dispatch import dynamic_dispatcher
from simpleflow.utils import json_dumps
from .stats import pretty

__all__ = [
    'show_workflow_profile',
    'show_workflow_status',
    'list_workflow_executions',
    'swf_identity',
]


def get_workflow_execution(domain_name, workflow_id, run_id=None):
    def filter_execution(*args, **kwargs):
        if 'workflow_status' in kwargs:
            kwargs['status'] = kwargs.pop('workflow_status')
        return query.filter(*args, **kwargs)[0]

    domain = swf.models.Domain(domain_name)
    query = swf.querysets.WorkflowExecutionQuerySet(domain)

    action = filter_execution
    keywords = {
        'workflow_id': workflow_id,
    }
    if run_id:
        action = query.get
        keywords['run_id'] = run_id

    try:
        workflow_execution = action(**keywords)
    except (swf.exceptions.DoesNotExistError, IndexError):
        keywords['workflow_status'] = swf.models.WorkflowExecution.STATUS_CLOSED
        workflow_execution = action(**keywords)

    return workflow_execution


def show_workflow_info(domain_name, workflow_id, run_id=None):
    workflow_execution = get_workflow_execution(
        domain_name,
        workflow_id,
        run_id,
    )
    return pretty.info(workflow_execution)


def show_workflow_profile(domain_name, workflow_id, run_id=None, nb_tasks=None):
    workflow_execution = get_workflow_execution(
        domain_name,
        workflow_id,
        run_id,
    )
    return pretty.profile(workflow_execution, nb_tasks)


def show_workflow_status(domain_name, workflow_id, run_id=None, nb_tasks=None):
    workflow_execution = get_workflow_execution(
        domain_name,
        workflow_id,
        run_id,
    )
    return pretty.status(workflow_execution, nb_tasks)


def list_workflow_executions(domain_name, *args, **kwargs):
    domain = swf.models.Domain(domain_name)
    query = swf.querysets.WorkflowExecutionQuerySet(domain)
    executions = query.all(*args, **kwargs)

    return pretty.list_executions(executions)


def filter_workflow_executions(domain_name, status, tag,
                               workflow_id, workflow_type_name,
                               workflow_type_version, *args, **kwargs):
    domain = swf.models.Domain(domain_name)
    query = swf.querysets.WorkflowExecutionQuerySet(domain)
    executions = query.filter(status, tag,
                              workflow_id, workflow_type_name,
                              workflow_type_version, *args, **kwargs)

    return pretty.list_details(executions)


def find_activity(history, scheduled_id=None, activity_id=None, input=None):
    """
    Finds an activity in a given workflow execution and returns a callable,
    some args and some kwargs so we can re-execute it.

    :type history: simpleflow.history.History
    :type scheduled_id: str
    :type activity_id: str
    :type input: Optional[dict[str, Any]]
    """
    found_activity = None
    for _, params in iteritems(history.activities):
        if params["scheduled_id"] == scheduled_id:
            found_activity = params
        if params["id"] == activity_id:
            found_activity = params

    if not found_activity:
        raise ValueError("Couldn't find activity.")

    # get the activity
    activity_str = found_activity["name"]
    dispatcher = dynamic_dispatcher.Dispatcher()
    activity = dispatcher.dispatch_activity(activity_str)

    # get the input
    input_ = input or found_activity["input"]
    if input_ is None:
        input_ = {}
    args = input_.get('args', ())
    kwargs = input_.get('kwargs', {})
    meta = input_.get('meta', {})

    # return everything
    return activity, args, kwargs, meta, found_activity


def get_task(domain_name, workflow_id, task_id, details):
    workflow_execution = get_workflow_execution(
        domain_name,
        workflow_id,
    )
    return pretty.get_task(workflow_execution, task_id, details)


def swf_identity():
    # basic identity
    identity = {
        'user': getpass.getuser(),          # system's user
        'hostname': socket.gethostname(),   # main hostname
        'pid': os.getpid(),                 # current pid
    }

    # adapt with extra keys from env
    if "SIMPLEFLOW_IDENTITY" in os.environ:
        try:
            extra_keys = json.loads(os.environ["SIMPLEFLOW_IDENTITY"])
        except Exception:
            extra_keys = {}
        for key, value in iteritems(extra_keys):
            identity[key] = value

    # remove null values
    identity = {k: v for k, v in iteritems(identity) if v is not None}

    # serialize the result
    return json_dumps(identity)
