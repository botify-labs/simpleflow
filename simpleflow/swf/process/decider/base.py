from __future__ import absolute_import

import logging
import multiprocessing
import os

import swf.actors
import swf.exceptions
import swf.models.decision

from simpleflow.process import Supervisor, with_state
from simpleflow.swf.process import Poller
from simpleflow.swf.utils import DecisionsAndContext


if False:
    from typing import Any, List, Optional, Union  # NOQA
    from swf.responses import Response  # NOQA
    from simpleflow.swf.executor import Executor  # NOQA


logger = logging.getLogger(__name__)


class Decider(Supervisor):
    """
    Decider.

    :ivar _poller: decider poller.
    :type _poller: DeciderPoller
    """
    def __init__(self, poller, nb_children=None):
        self._poller = poller
        super(Decider, self).__init__(
            payload=self._poller.start,
            nb_children=nb_children,
        )


class DeciderPoller(Poller, swf.actors.Decider):
    """
    Decider poller.

    :ivar workflow_name: concatenated workflows names.
    :type workflow_name: str
    :ivar _workflow_executors: executors dict: workflow name -> executor
    :type _workflow_executors: Dict[str, Executor]
    :ivar nb_retries: # of retries allowed
    :type nb_retries: int
    """
    def __init__(self,
                 workflow_executors,  # type: List[Executor]
                 domain,  # type: swf.models.Domain
                 task_list,  # type: str
                 is_standalone,  # type: bool
                 nb_retries=3,  # type: int
                 *args,
                 **kwargs
                 ):
        # type: (...) -> None
        """
        The decider is an actor that reads the full history of the workflow
        execution and decides what happens next. The :class:`DeciderPoller`
        polls decision tasks from a task list and send them to a worker that
        returns one or several decisions.  A decision is for example scheduling
        an activity or completing the workflow execution.

        SWF ensures that only one decider gets a decision task for a workflow
        execution. A decider is stateless because it takes decisions solely
        based upon the history that comes with the decision task.

        This implementation polls a single task list within a single domain.
        It can handle several workflows on the same task list. The rationale
        behind this is to limit operational burden by having a single service
        handling multiple workflows.

        :param workflow_executors: executors handling workflow executions.
        :type  workflow_executors: list[simpleflow.swf.executor.Executor]

        """
        self.workflow_name = '{}'.format(','.join(
            [
                ex.workflow_class.name for ex in workflow_executors
                ]))

        # Maps a workflow's name to its definition.
        # Used to dispatch a decision task to the corresponding workflow.
        self._workflow_executors = {
            executor.workflow_class.name: executor for executor in workflow_executors
        }

        if task_list:
            self.task_list = task_list
        else:
            self.task_list = workflow_executors[0].workflow_class.task_list
            # If not passed explicitly, all executors must use the same task list
            # else it's probably a mistake so we raise an error.
            self._check_all_task_lists_identical()

        self.nb_retries = nb_retries
        self.domain = domain
        self.is_standalone = is_standalone

        # All executors must have the same domain.
        self._check_all_domains_identical()

        super(DeciderPoller, self).__init__(domain, self.task_list)

    def __repr__(self):
        return '{cls}({domain}, {task_list}, {workflows})'.format(
            cls=self.__class__.__name__,
            domain=self.domain.name,
            task_list=self.task_list,
            workflows=','.join(self._workflow_executors),
        )

    def _check_all_domains_identical(self):
        for ex in self._workflow_executors.values():
            if ex.domain.name != self.domain.name:
                raise ValueError(
                    'all workflows must be in the same domain "{}"'.format(
                        self.domain.name))

    def _check_all_task_lists_identical(self):
        for ex in self._workflow_executors.values():
            if ex.workflow_class.task_list != self.task_list:
                raise ValueError(
                    'all workflows must have the same task list '
                    '"{}" unless you specify it explicitly'.format(
                        self.task_list))

    @property
    def name(self):
        """
        The main purpose of this property is to find what workflow a decider
        handles.

        :rtype: str
        """
        if self.workflow_name:
            suffix = '(workflow={})'.format(self.workflow_name)
        else:
            suffix = ''
        return '{}{}'.format(self.__class__.__name__, suffix)

    @with_state('polling')
    def poll(self, task_list=None, identity=None, **kwargs):
        return swf.actors.Decider.poll(self, task_list, identity, **kwargs)

    @with_state('completing')
    def complete(self, token, decisions=None, execution_context=None):
        # type: (str, Optional[List], Union[Optional[Any], DecisionsAndContext], Optional) -> None
        """
        DubiousImpl: ~same signature as swf.actors.Decider.complete although execution_context is never set...
        :param token: task token.
        :param decisions: decisions, maybe with context.
        :param execution_context: None...
        :return: nothing.
        """
        if isinstance(decisions, DecisionsAndContext):
            decisions, execution_context = decisions.decisions, decisions.execution_context
        return swf.actors.Decider.complete(self, token, decisions, execution_context)

    @with_state('processing')
    def process(self, decision_response):
        """
        Take a PollForDecisionTask response object and try to complete the
        decision task, by calling self._complete() with the response token and
        a set of decisions. We fork so it protects us reliably against memory
        leaks on long-running deciders.

        :param decision_response: an object wrapping the PollForDecisionTask response.
        :type  decision_response: swf.responses.Response
        """
        spawn(self, decision_response)

    @with_state('deciding')
    def decide(self, decision_response):
        """
        Delegate the decision to the decider worker (which itself delegates to
        the executor).

        :param decision_response: an object wrapping the PollForDecisionTask response.
        :type  decision_response: swf.responses.Response

        :return: the decisions.
        :rtype: Union[List[swf.models.decision.base.Decision], DecisionsAndContext]
        """
        worker = DeciderWorker(self.domain, self._workflow_executors)
        decisions = worker.decide(decision_response, self.task_list if self.is_standalone else None)
        return decisions


class DeciderWorker(object):
    """
    Decider worker.
    :ivar _domain: SWF domain.
    :type _domain: swf.models.Domain
    :ivar _workflow_executors: executors.
    :type _workflow_executors: dict[str, simpleflow.swf.executor.Executor]
    """

    def __init__(self, domain, workflow_executors):
        self._domain = domain
        self._workflow_executors = workflow_executors

    def decide(self, decision_response, task_list):
        """
        Delegate the decision to the executor, loading it if needed.

        :param decision_response: an object wrapping the PollForDecisionTask response.
        :type  decision_response: swf.responses.Response
        :param task_list:
        :type task_list: Optional[str]

        :returns: the decisions.
        :rtype: list[swf.models.decision.base.Decision]
        """
        history = decision_response.history
        workflow_name = history[0].workflow_type['name']
        workflow_executor = self._workflow_executors.get(workflow_name)
        if not workflow_executor:
            # Child workflow from another module
            from . import helpers
            workflow_executor = helpers.load_workflow_executor(
                self._domain,
                workflow_name,
                task_list=task_list,
            )
            self._workflow_executors[workflow_name] = workflow_executor
        try:
            decisions = workflow_executor.replay(decision_response)
        except Exception as err:
            import traceback
            details = traceback.format_exc()
            message = "workflow decision failed: {}".format(err)
            logger.exception(message)
            decision = swf.models.decision.WorkflowExecutionDecision()
            decision.fail(reason=message, details=details)
            decisions = [decision]

        return decisions


def process_decision(poller, decision_response):
    # type: (DeciderPoller, Response) -> None
    logger.debug("process_decision() pid={}".format(os.getpid()))
    logger.info("taking decision for workflow {}".format(poller.workflow_name))
    decisions = poller.decide(decision_response)
    try:
        logger.info("completing decision for workflow {}".format(poller.workflow_name))
        poller.complete_with_retry(decision_response.token, decisions)
    except Exception as err:
        logger.error("cannot complete decision: {}".format(err))


def spawn(poller, decision_response):
    logger.debug("spawn() pid={}".format(os.getpid()))
    worker = multiprocessing.Process(
        target=process_decision,
        args=(poller, decision_response),
    )
    worker.start()
    worker.join()
