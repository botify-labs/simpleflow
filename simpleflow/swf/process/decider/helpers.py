import logging

import swf.models

from simpleflow.swf.executor import Executor
from . import (
    Decider,
    DeciderPoller,
)

logger = logging.getLogger(__name__)


def load_workflow_executor(domain, workflow_name, task_list=None, repair_with=None,
                           force_activities=None):
    """
    Load a workflow executor.

    :param domain:
    :type domain: str | swf.models.Domain
    :param workflow_name:
    :type workflow_name: str
    :param task_list:
    :type task_list: Optional[str]
    :param repair_with:
    :type repair_with: Optional[simpleflow.history.History]
    :param force_activities:
    :type force_activities: Optional[str]
    :return: Executor for this workflow
    :rtype: Executor
    """
    logger.debug('load_workflow_executor(workflow_name="{}")'.format(workflow_name))
    module_name, object_name = workflow_name.rsplit('.', 1)
    module = __import__(module_name, fromlist=['*'])

    workflow = getattr(module, object_name)

    # TODO: find the cause of this differentiated behaviour
    if not isinstance(domain, swf.models.Domain):
        domain = swf.models.Domain(domain)

    return Executor(domain, workflow, task_list,
                    repair_with=repair_with, force_activities=force_activities)


def make_decider_poller(workflows, domain, task_list, repair_with=None,
                        force_activities=None,
                        is_standalone=False):
    """
    Factory building a decider poller.
    :param workflows:
    :type workflows:
    :param domain:
    :type domain:
    :param task_list:
    :type task_list:
    :param repair_with:
    :type repair_with: Optional[simpleflow.history.History]
    :param force_activities:
    :type force_activities: Optional[str]
    :param is_standalone: Whether the executor use this task list (and pass it to the workers)
    :type is_standalone: bool
    :return:
    :rtype: DeciderPoller
    """
    if repair_with and len(workflows) != 1:
        # too complicated ; I even wonder why passing multiple workflows here is
        # useful, a domain+task_list is typically handled in a single workflow
        # definition, seems like good practice (?)
        raise ValueError("Sorry you can't repair more than 1 workflow at once!")

    executors = [
        load_workflow_executor(domain, workflow, task_list if is_standalone else None,
                               repair_with=repair_with,
                               force_activities=force_activities)
        for workflow in workflows
        ]
    domain = swf.models.Domain(domain)
    return DeciderPoller(executors, domain, task_list)


def make_decider(workflows, domain, task_list, nb_children=None,
                 repair_with=None, force_activities=None,
                 is_standalone=False):
    """
    Instantiate a Decider.
    :param workflows:
    :type workflows: list[str]
    :param domain:
    :type domain: str
    :param task_list:
    :type task_list: str
    :param nb_children:
    :type nb_children: Optional[int]
    :param repair_with: previous history
    :type repair_with: Optional[simpleflow.history.History]
    :param force_activities: Regex matching the activities to force
    :type force_activities: Optional[str]
    :param is_standalone: Whether the executor use this task list (and pass it to the workers)
    :type is_standalone: bool
    :return:
    :rtype: Decider
    """
    poller = make_decider_poller(workflows, domain, task_list,
                                 repair_with=repair_with,
                                 force_activities=force_activities,
                                 is_standalone=is_standalone,
                                 )
    return Decider(poller, nb_children=nb_children)
