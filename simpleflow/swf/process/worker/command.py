from __future__ import absolute_import

import swf.models

from .base import (
    Worker,
    ActivityPoller,
)


def make_worker_poller(workflow_id, domain, task_list, heartbeat):
    """

    :param workflow_id:
    :type workflow_id:
    :param domain:
    :type domain:
    :param task_list:
    :type task_list:
    :param heartbeat:
    :type heartbeat:
    :return:
    :rtype:
    """
    domain = swf.models.Domain(domain)
    return ActivityPoller(
        workflow_id,
        domain,
        task_list,
        heartbeat,
    )


def start(workflow_id, domain, task_list, nb_processes=None, heartbeat=60):
    """

    :param workflow_id:
    :type workflow_id:
    :param domain:
    :type domain:
    :param task_list:
    :type task_list:
    :param nb_processes:
    :type nb_processes:
    :param heartbeat:
    :type heartbeat:
    :return:
    :rtype:
    """
    poller = make_worker_poller(workflow_id, domain, task_list, heartbeat)
    worker = Worker(poller, nb_processes)
    worker.is_alive = True
    worker.start()
