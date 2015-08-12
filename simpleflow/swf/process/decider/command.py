from __future__ import absolute_import

from . import Decider


def start(workflows, domain, task_list, log_level=None, nb_processes=None):
    decider = Decider.make(workflows, domain, task_list, nb_processes)
    decider.is_alive = True
    decider.start()
