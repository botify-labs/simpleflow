from __future__ import annotations

import swf.models
from simpleflow import logger
from simpleflow.swf.executor import Executor
from simpleflow.utils import import_from_module

from . import Decider, DeciderPoller


def load_workflow_executor(
    domain,
    workflow_name,
    task_list=None,
    repair_with=None,
    force_activities=None,
    repair_workflow_id=None,
    repair_run_id=None,
):
    """
    Load a workflow executor.

    :param domain:
    :type domain: swf.models.Domain
    :param workflow_name:
    :type workflow_name: str
    :param task_list:
    :type task_list: Optional[str]
    :param repair_with:
    :type repair_with: Optional[simpleflow.history.History]
    :param force_activities:
    :type force_activities: Optional[str]
    :param repair_workflow_id: workflow ID to repair
    :type repair_workflow_id: Optional[str]
    :param repair_run_id: run ID to repair
    :type repair_run_id: Optional[str]
    :return: Executor for this workflow
    :rtype: Executor
    """
    logger.debug(f'load_workflow_executor(workflow_name="{workflow_name}")')
    workflow = import_from_module(workflow_name)

    if not isinstance(domain, swf.models.Domain):
        raise ValueError(f"domain is a {type(domain).__name__}, not a Domain")

    return Executor(
        domain,
        workflow,
        task_list,
        repair_with=repair_with,
        force_activities=force_activities,
        repair_workflow_id=repair_workflow_id,
        repair_run_id=repair_run_id,
    )


def make_decider_poller(
    workflows,
    domain,
    task_list,
    repair_with=None,
    force_activities=None,
    is_standalone=False,
    repair_workflow_id=None,
    repair_run_id=None,
):
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
    :param repair_workflow_id: workflow ID to repair
    :type repair_workflow_id: Optional[str]
    :param repair_run_id: run ID to repair
    :type repair_run_id: Optional[str]
    :return:
    :rtype: DeciderPoller
    """
    if repair_with and len(workflows) != 1:
        # too complicated ; I even wonder why passing multiple workflows here is
        # useful, a domain+task_list is typically handled in a single workflow
        # definition, seems like good practice (?)
        raise ValueError("Sorry you can't repair more than 1 workflow at once!")

    domain = swf.models.Domain(domain)
    executors = [
        load_workflow_executor(
            domain,
            workflow,
            task_list if is_standalone else None,
            repair_with=repair_with,
            force_activities=force_activities,
            repair_workflow_id=repair_workflow_id,
            repair_run_id=repair_run_id,
        )
        for workflow in workflows
    ]
    return DeciderPoller(executors, domain, task_list, is_standalone)


def make_decider(
    workflows,
    domain,
    task_list,
    nb_children=None,
    repair_with=None,
    force_activities=None,
    is_standalone=False,
    repair_workflow_id=None,
    repair_run_id=None,
):
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
    :param repair_workflow_id: workflow ID to repair
    :type repair_workflow_id: Optional[str]
    :param repair_run_id: run ID to repair
    :type repair_run_id: Optional[str]
    :return:
    :rtype: Decider
    """
    poller = make_decider_poller(
        workflows,
        domain,
        task_list,
        repair_with=repair_with,
        force_activities=force_activities,
        is_standalone=is_standalone,
        repair_workflow_id=repair_workflow_id,
        repair_run_id=repair_run_id,
    )
    return Decider(poller, nb_children=nb_children)
