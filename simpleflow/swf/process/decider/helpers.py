from __future__ import annotations

from typing import TYPE_CHECKING

import simpleflow.swf.mapper.models
from simpleflow import logger
from simpleflow.swf.executor import Executor
from simpleflow.utils import import_from_module

from .base import Decider, DeciderPoller

if TYPE_CHECKING:
    from simpleflow.history import History


def load_workflow_executor(
    domain: simpleflow.swf.mapper.models.Domain,
    workflow_name: str,
    task_list: str | None = None,
    repair_with: History | None = None,
    force_activities: str | None = None,
    repair_workflow_id: str | None = None,
    repair_run_id: str | None = None,
) -> Executor:
    """
    Load a workflow executor.
    """
    logger.debug(f'load_workflow_executor(workflow_name="{workflow_name}")')
    workflow = import_from_module(workflow_name)

    if not isinstance(domain, simpleflow.swf.mapper.models.Domain):
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
    workflows: list[str],
    domain_name: str,
    task_list: str,
    repair_with: History | None = None,
    force_activities: str | None = None,
    is_standalone: bool = False,
    repair_workflow_id: str | None = None,
    repair_run_id: str | None = None,
) -> DeciderPoller:
    """
    Factory building a decider poller.
    """
    if repair_with and len(workflows) != 1:
        # too complicated; I even wonder why passing multiple workflows here is
        # useful, a domain+task_list is typically handled in a single workflow
        # definition, seems like good practice (?)
        raise ValueError("Sorry you can't repair more than 1 workflow at once!")

    domain = simpleflow.swf.mapper.models.Domain(domain_name)
    if not domain.exists:
        domain.save()
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
    workflows: list[str],
    domain: str,
    task_list: str,
    nb_children: int | None = None,
    repair_with: History | None = None,
    force_activities: str | None = None,
    is_standalone: bool = False,
    repair_workflow_id: str | None = None,
    repair_run_id: str | None = None,
) -> Decider:
    """
    Instantiate a Decider.
    repair_with: previous history
    force_activities: Regex matching the activities to force
    is_standalone: Whether the executor uses this task list (and pass it to the workers)
    repair_workflow_id: workflow ID to repair
    repair_run_id: run ID to repair
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
