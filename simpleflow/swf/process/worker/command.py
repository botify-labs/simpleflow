from __future__ import absolute_import

import swf.models

from .base import (
    Worker,
    ActivityPoller,
)


def make_worker_poller(domain, task_list, heartbeat):
    """

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
    return ActivityPoller(domain, task_list, heartbeat, heartbeat)


def start(domain, task_list, nb_processes=None, heartbeat=60):
    """

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
    poller = make_worker_poller(domain, task_list, heartbeat)
    worker = Worker(poller, nb_processes)
    worker.is_alive = True
    worker.start()
