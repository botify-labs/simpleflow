from __future__ import annotations

from simpleflow import logger

from . import helpers


def start(
    workflows,
    domain,
    task_list,
    log_level=None,
    nb_processes=None,
    repair_with=None,
    force_activities=None,
    is_standalone=False,
    repair_workflow_id=None,
    repair_run_id=None,
):
    """
    Start a decider.
    :param workflows:
    :type workflows: list[str]
    :param domain:
    :type domain:
    :param task_list:
    :type task_list:
    :param log_level:
    :type log_level:
    :param nb_processes:
    :type nb_processes:
    :param repair_with:
    :type repair_with: Optional[simpleflow.history.History]
    :param force_activities:
    :type force_activities:
    :param is_standalone: Whether the executor use this task list (and pass it to the workers)
    :type is_standalone: bool
    :param repair_workflow_id: workflow ID to repair
    :type repair_workflow_id: Optional[str]
    :param repair_run_id: run ID to repair
    :type repair_run_id: Optional[str]
    """
    if log_level:
        logger.warning("Deprecated: --log-level will be removed, use LOG_LEVEL environment variable instead")
    decider = helpers.make_decider(
        workflows,
        domain,
        task_list,
        nb_processes,
        repair_with=repair_with,
        force_activities=force_activities,
        is_standalone=is_standalone,
        repair_workflow_id=repair_workflow_id,
        repair_run_id=repair_run_id,
    )
    decider.is_alive = True
    decider.start()
