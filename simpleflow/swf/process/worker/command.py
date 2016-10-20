from __future__ import absolute_import

import swf.models

from simpleflow.swf.process.decider import helpers
from .base import (
    Worker,
    ActivityPoller,
)


def make_worker_poller(workflow, domain, task_list, heartbeat):
    """
    Make a worker poller for the domain and task list.
    :param domain:
    :type domain: str
    :param task_list:
    :type task_list: str
    :param heartbeat:
    :type heartbeat: int
    :return:
    :rtype: ActivityPoller
    """
    domain = swf.models.Domain(domain)
    return ActivityPoller(
        domain,
        task_list,
        helpers.load_workflow(domain, workflow),
        heartbeat,
    )


def start(workflow, domain, task_list, nb_processes=None, heartbeat=60):
    """
    Start a worker for the given domain and task_list.
    :param domain:
    :type domain: str
    :param task_list:
    :type task_list: str
    :param nb_processes: Number of processes. Default: number of CPUs
    :type nb_processes: Optional[int]
    :param heartbeat: heartbeat frequency in seconds
    :type heartbeat: int
    """
    poller = make_worker_poller(workflow, domain, task_list, heartbeat)
    worker = Worker(poller, nb_processes)
    worker.is_alive = True
    worker.start()
