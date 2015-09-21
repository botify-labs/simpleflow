from __future__ import absolute_import

import logging

import swf.actors
import swf.exceptions

from simpleflow.swf.process.actor import (
    Supervisor,
    Poller,
    with_state,
)
from . import helpers


logger = logging.getLogger(__name__)


class Decider(Supervisor):
    def __init__(self, poller, nb_children=None):
        self._poller = poller
        self._poller.is_alive = True
        Supervisor.__init__(
            self,
            payload=self._poller.start,
            nb_children=nb_children,
        )

    @classmethod
    def make(cls, workflows, domain, task_list, nb_children=None):
        poller = DeciderPoller.make(workflows, domain, task_list)
        return Decider(poller, nb_children=nb_children)


class DeciderPoller(swf.actors.Decider, Poller):
    def __init__(self, workflows, domain, task_list, nb_retries=3,
                 *args, **kwargs):
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

        :param workflows: that handles workflow executions.
        :type  workflows: [simpleflow.swf.Executor].

        """
        self._workflow_name = '{}'.format(','.join([
            ex._workflow.name for ex in workflows
        ]))

        # Maps a workflow's name to its definition.
        # Used to dispatch a decision task to the corresponding workflow.
        self._workflows = {
            executor._workflow.name: executor for executor in workflows
        }

        # All executors must have the same domain and task list.
        for ex in workflows[1:]:
            if ex.domain.name != domain.name:
                raise ValueError(
                    'all workflows must be in the same domain "{}"'.format(
                        domain.name))
            elif ex._workflow.task_list != task_list:
                raise ValueError(
                    'all workflows must have the same task list "{}"'.format(
                        task_list))

        self.nb_retries = nb_retries

        Poller.__init__(
            self,
            domain,
            task_list,
            *args,    # directly forward them.
            **kwargs  # directly forward them.
        )

    def __repr__(self):
        return '{cls}({domain}, {task_list}, {workflows})'.format(
            cls=self.__class__.__name__,
            domain=self.domain.name,
            task_list=self.task_list,
            workflows=','.join(self._workflows),
        )

    @classmethod
    def make(cls, workflows, domain, task_list):
        """Factory to build a decider."""
        executors = [
            helpers.load_workflow(domain, workflow, task_list) for
            workflow in workflows
        ]
        domain = swf.models.Domain(domain)
        return cls(executors, domain, task_list)

    @property
    def name(self):
        """
        The main purpose of this property is to find what workflow a decider
        handles.

        """
        if self._workflow_name:
            suffix = '(workflow={})'.format(self._workflow_name)
        else:
            suffix = ''
        return '{}{}'.format(self.__class__.__name__, suffix)

    @with_state('polling')
    def poll(self, task_list, identity):
        return swf.actors.Decider.poll(self, task_list, identity)

    @with_state('completing decision task')
    def complete(self, token, decisions):
        return swf.actors.Decider.complete(self, token, decisions)

    def process(self, task):
        token, history = task

        logger.info('taking decision for workflow {}'.format(
            self._workflow_name))
        decisions = self.decide(history)
        try:
            logger.info('completing decision for workflow {}'.format(
                self._workflow_name))
            self._complete(token, decisions)
        except Exception as err:
            logger.error('cannot complete decision: {}'.format(err))

    @with_state('deciding')
    def decide(self, history):
        worker = DeciderWorker(self.domain, self._workflows)
        decisions = worker.decide(history)
        return decisions


class DeciderWorker(object):
    def __init__(self, domain, workflows):
        self._domain = domain
        self._workflow_name = None
        self._workflows = workflows

    def decide(self, history):
        """
        Delegate the decision to the executor.

        :param history: of the workflow execution.
        :type  history: swf.models.History.
        :returns:
            :rtype: (str, [swf.models.decision.base.Decision])

        """
        workflow_name = history[0].workflow_type['name']
        workflow_executor = self._workflows.get(workflow_name)
        if not workflow_executor:
            workflow_executor = helpers.load_workflow(
                self._domain,
                workflow_name,
            )
            self._workflows[workflow_name] = workflow_executor
        self._workflow_name = workflow_name
        try:
            decisions = workflow_executor.replay(history)
            if isinstance(decisions, tuple) and len(decisions) == 2:  # (decisions, context)
                decisions = decisions[0]
        except Exception as err:
            message = "workflow decision failed: {}".format(err)
            logger.error(message)
            decision = swf.models.decision.WorkflowExecutionDecision()
            decision.fail(reason=swf.format.reason(message))
            decisions = [decision]

        return decisions
