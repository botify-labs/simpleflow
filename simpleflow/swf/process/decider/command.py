from __future__ import absolute_import
import logging

from . import helpers


logger = logging.getLogger(__name__)


def start(workflows, domain, task_list, log_level=None, nb_processes=None,
          repair_with=None, force_activities=None, is_standalone=False):
    """
    Start a decider.
    :param workflows:
    :type workflows:
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
    """
    if log_level:
        logger.warning(
            "Deprecated: --log-level will be removed, use LOG_LEVEL environment variable instead"
        )
    decider = helpers.make_decider(
        workflows, domain, task_list, nb_processes,
        repair_with=repair_with,
        force_activities=force_activities,
        is_standalone=is_standalone,
    )
    decider.is_alive = True
    decider.start()
