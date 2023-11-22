from __future__ import annotations

import simpleflow.swf.mapper.models

from .base import ActivityPoller, Worker


def make_worker_poller(
    domain: str,
    task_list: str,
    middlewares: dict[str, list[str]] | None,
    heartbeat: int,
    poll_data: str,
) -> ActivityPoller:
    """
    Make a worker poller for the domain and task list.
    """
    domain = simpleflow.swf.mapper.models.Domain(domain)
    return ActivityPoller(
        domain=domain,
        task_list=task_list,
        middlewares=middlewares,
        heartbeat=heartbeat,
        poll_data=poll_data,
    )


def start(
    domain: str,
    task_list: str,
    middlewares: dict[str, str] | None = None,
    nb_processes: int | None = None,
    heartbeat: int = 60,
    one_task: bool = False,
    poll_data: str | None = None,
):
    """
    Start a worker for the given domain and task_list.
    middlewares: Paths to middleware functions to execute before and after any Activity
    nb_processes: Number of processes. Default: number of CPUs
    heartbeat: heartbeat frequency in seconds
    one_task: Process only one task then shutdown
    poll_data: Base64 encoded poll data from SWF, in case you don't want to poll directly.
    """
    poller = make_worker_poller(
        domain=domain,
        task_list=task_list,
        middlewares=middlewares,
        heartbeat=heartbeat,
        poll_data=poll_data,
    )

    if poll_data:
        # if "poll_data" is provided, no need to process it multiple times
        one_task = True

    if one_task:
        poller.run_once()
    else:
        worker = Worker(poller, nb_processes)
        worker.is_alive = True
        worker.start()
