import logging
import multiprocessing
import os
import json

import swf.actors
import swf.format

from simpleflow.swf.process.actor import (
    MultiProcessActor,
)
from simpleflow.utils import retry


logger = logging.getLogger(__name__)


class Worker(swf.actors.ActivityWorker, MultiProcessActor):
    def __init__(self, domain, task_list,
                 dispatcher=DEFAULT_DISPATCHER,
                 heartbeat_interval=50,
                 nb_children=None,
                 *args, **kwargs):
        self.dispatcher = dispatcher
        self._heartbeat_interval = heartbeat_interval

        MultiProcessActor.__init__(
            self,
            domain,
            task_list,
            nb_children=nb_children,
            *args,    # directly forward them.
            **kwargs  # directly forward them.
        )

    @property
    def state_self(self):
        return self._state

    @property
    def name(self):
        if self._task:
            suffix = ':'.format(self._task.activity_id)
        else:
            suffix = ''
        return '{}(domain={}, task_list={}){}'.format(
            self.__class__.__name__,
            self.domain,
            self.task_list,
            suffix,
        )

    def fail(self, task, token, reason=None, details=None):
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

    def dispatch(self, task):
        name = task.activity_type.name
        return self.dispatcher.dispatch(name)

    def process_task(self, task):
        handler = self.dispatch(task)
        input = json.loads(task.input)
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})
        return handler(*args, **kwargs)

    @reset_signal_handlers
    @will_release_semaphore
    def handle_activity_task(self, task_list=None):
        """
        Happens in a subprocess. Polls and make decisions with respect to the
        current state of the workflow execution represented by its history.

        """
        self.state = 'polling'
        try:
            token, task= self.poll(task_list)
        except swf.exceptions.PollTimeout:
            # TODO(ggreg) move this out of the task handling logic.
            self._children_processes.remove(os.getpid())
            return

        self.state = 'processing'
        try:
            result = self.process_task(task)
        except Exception as err:
            message = "activity task failed: {}".format(err)
            logger.error(message)
            decision = swf.models.decision.WorkflowExecutionDecision()
            self.fail(reason=swf.format.reason(message))
            decisions = [decision]

        try:
            self.state = 'completing'
            complete = retry.with_delay(
                nb_times=self.nb_retries,
                delay=retry.exponential,
                logger=logger,
            )(self.complete)  # Exponential backoff on errors.
            complete(token, decisions)
        except Exception as err:
            # This is embarassing because the worker cannot notify SWF of the
            # task completion. As it will not try again, the activity task will
            # timeout (start_to_complete).
            logger.error("cannot complete activity task: %s", str(err))

        # TODO(ggreg) move this out of the decision handling logic.
        # This cannot work because it is executed in a subprocess.
        # Hence it does not share the parent's object.
        self._children_processes.remove(os.getpid())

    def spawn_handler(self):
        try:
            self._semaphore.acquire()
        except OSError as err:
            logger.warning("cannot acquire semaphore: %s", str(err))

        if self.is_alive:
            process = multiprocessing.Process(target=self.handle_activity_task)
            process.start()
            # This is needed to wait for children when stopping the main
            # decider process.
            self._children_processes.add(process)

    def start(self):
        logger.info(
            'starting %s on domain %s',
            self.name,
            self.domain.name,
        )
        self.set_process_name()
        while self.is_alive():
            self.spawn_handler()
