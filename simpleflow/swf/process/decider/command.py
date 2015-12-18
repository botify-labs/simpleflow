from __future__ import absolute_import
import logging

from . import helpers


logger = logging.getLogger(__name__)


def start(workflows, domain, task_list, log_level=None, nb_processes=None):
    if log_level:
        logger.warning(
            "Deprecated: --log-level will be removed, use LOG_LEVEL environment variable instead"
        )
    decider = helpers.make_decider(workflows, domain, task_list, nb_processes)
    decider.is_alive = True
    decider.start()
