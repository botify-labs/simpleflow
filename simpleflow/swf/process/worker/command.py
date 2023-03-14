from __future__ import annotations

import swf.models

from .base import ActivityPoller, Worker


def make_worker_poller(domain, task_list, middlewares, heartbeat, process_mode, poll_data):
    """
    Make a worker poller for the domain and task list.
    :param domain:
    :type domain: str
    :param task_list:
    :type task_list: str
    :param heartbeat:
    :type heartbeat: int
    :param process_mode: Whether to process locally (default) or spawn a Kubernetes job.
    :type process_mode: str
    :param poll_data: Base64 encoded poll data from SWF, in case you don't want to poll directly.
    :type poll_data: str
    :return:
    :rtype: ActivityPoller
    """
    domain = swf.models.Domain(domain)
    return ActivityPoller(domain, task_list, middlewares, heartbeat, process_mode, poll_data)


def start(
    domain,
    task_list,
    middlewares=None,
    nb_processes=None,
    heartbeat=60,
    one_task=False,
    process_mode=None,
    poll_data=None,
):
    """
    Start a worker for the given domain and task_list.
    :param domain:
    :type domain: str
    :param task_list:
    :type task_list: str
    :param middlewares: Paths to middleware functions to execute before and after any Activity
    :type middlewares: Optional[Dict[str, str]]
    :param nb_processes: Number of processes. Default: number of CPUs
    :type nb_processes: Optional[int]
    :param heartbeat: heartbeat frequency in seconds
    :type heartbeat: Optional[int]
    :param one_task: Process only one task then shutdown
    :type one_task: Optional[bool]
    :param process_mode: Whether to process locally (default) or spawn a Kubernetes job.
    :type process_mode: Optional[str]
    :param poll_data: Base64 encoded poll data from SWF, in case you don't want to poll directly.
    :type poll_data: Optional[str]
    """
    poller = make_worker_poller(domain, task_list, middlewares, heartbeat, process_mode, poll_data)

    if poll_data:
        # if "poll_data" is provided, no need to process it multiple times
        one_task = True

    if one_task:
        poller.run_once()
    else:
        worker = Worker(poller, nb_processes)
        worker.is_alive = True
        worker.start()
