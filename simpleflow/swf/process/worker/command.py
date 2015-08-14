from __future__ import absolute_import

import swf.models

from simpleflow.swf.process.decider import helpers
from .base import (
    Worker,
    ActivityPoller,
)


def make_worker_poller(workflow, domain, task_list, heartbeat):
    domain = swf.models.Domain(domain)
    return ActivityPoller(
        domain,
        task_list,
        helpers.load_workflow(domain, workflow),
        heartbeat,
    )


def start(workflow, domain, task_list, nb_processes=None, heartbeat=60):
    poller = make_worker_poller(workflow, domain, task_list, heartbeat)
    worker = Worker(poller, nb_processes)
    worker.is_alive = True
    worker.start()
