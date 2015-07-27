import logging
import json
import traceback
import multiprocessing

import swf.actors
import swf.format

import simpleflow
from simpleflow.swf.process.actor import (
    Supervisor,
    Poller,
    with_state,
)
from .dispatch import from_task_registry


logger = logging.getLogger(__name__)


class Worker(Supervisor):
    def __init__(self, poller, nb_children=None):
        self._poller = poller
        self._poller.is_alive = True
        Supervisor.__init__(
            self,
            payload=self._poller.start,
            nb_children=nb_children,
        )


class ActivityPoller(Poller, swf.actors.ActivityWorker):
    """
    Polls an activity and handles it in the worker.

    """
    def __init__(self, domain, task_list, workflow,
                 *args, **kwargs):
        self._workflow = workflow
        self.nb_retries = 3

        swf.actors.ActivityWorker.__init__(
            self,
            domain,
            task_list,
            *args,    # directly forward them.
            **kwargs  # directly forward them.
        )

    @property
    def name(self):
        return '{}({})'.format(
            self.__class__.__name__,
            self._workflow._workflow.name,
        )

    @with_state('polling')
    def poll(self, task_list, identity):
        return swf.actors.ActivityWorker.poll(self, task_list, identity)

    @with_state('processing task')
    def process(self, request):
        token, task = request
        worker = ActivityWorker(
            self.task_list,
            self._workflow,
        )
        try:
            result = worker.process(task)
        except Exception as err:
            tb = traceback.format_exc()
            logger.exception(err)
            return self.fail(token, task, reason=str(err), details=tb)

        try:
            self._complete(token, json.dumps(result))
        except Exception as err:
            logger.exception(err)
            reason = 'cannot complete task {}: {}'.format(
                task.activity_id,
                err,
            )
            self.fail(token, task, reason)

    @with_state('completing')
    def complete(self, token, result):
        swf.actors.ActivityWorker.complete(self, token, result)

    @with_state('failing')
    def fail(self, token, task, reason=None, details=None):
        try:
            return swf.actors.ActivityWorker.fail(
                self,
                token,
                reason=swf.format.reason(reason),
                details=swf.format.details(details),
            )
        except Exception as err:
            logger.error('cannot fail task {}: {}'.format(
                task.activity_type.name,
                err,
            ))


class ActivityWorker(object):
    def __init__(self, task_list, workflow):
        self._dispatcher = from_task_registry.RegistryDispatcher(
            simpleflow.task.registry,
            None,
            workflow,
        )

    def dispatch(self, task):
        name = task.activity_type.name
        return self._dispatcher.dispatch(name)

    def process(self, task):
        handler = self.dispatch(task)
        input = json.loads(task.input)
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})
        return handler(*args, **kwargs)
