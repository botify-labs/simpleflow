import swf.models

from simpleflow.swf.executor import Executor
from . import (
    Decider,
    DeciderPoller,
)


def load_workflow(domain, workflow_name, task_list=None):
    module_name, object_name = workflow_name.rsplit('.', 1)
    module = __import__(module_name, fromlist=['*'])

    workflow = getattr(module, object_name)
    return Executor(swf.models.Domain(domain), workflow, task_list)


def make_decider_poller(workflows, domain, task_list):
    """
    Factory to build a decider.

    """
    executors = [
        load_workflow(domain, workflow, task_list) for workflow in workflows
    ]
    domain = swf.models.Domain(domain)
    return DeciderPoller(executors, domain, task_list)


def make_decider(workflows, domain, task_list, nb_children=None):
    poller = make_decider_poller(workflows, domain, task_list)
    return Decider(poller, nb_children=nb_children)
