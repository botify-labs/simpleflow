import logging

import swf.models
import swf.models.decision

from simpleflow import task
from simpleflow.utils import json_dumps

logger = logging.getLogger(__name__)


class SwfTask(object):
    pass


class ActivityTask(task.ActivityTask, SwfTask):
    """
    Activity task managed on SWF.
    """
    @classmethod
    def from_generic_task(cls, task):
        """
        Casts a generic simpleflow.task.ActivityTask into a SWF one.
        """
        return cls(task.activity, *task._args, **task._kwargs)

    @property
    def task_list(self):
        return self.activity.task_list

    def schedule(self, domain, task_list=None, **kwargs):
        """
        Schedule an activity.

        :param domain:
        :type domain: swf.models.Domain
        :param task_list:
        :type task_list: Optional[str]
        :param kwargs:
        :return:
        :rtype: list[swf.models.decision.Decision]
        """
        activity = self.activity
        # FIXME Always involve a GET call to the SWF API which introduces useless
        # latency if the ActivityType already exists.
        model = swf.models.ActivityType(
            domain,
            activity.name,
            version=activity.version,
        )

        input = {
            'args': self.args,
            'kwargs': self.kwargs,
        }

        if task_list is None:
            task_list = activity.task_list
        task_timeout = kwargs.get(
            'task_timeout',
            activity.task_start_to_close_timeout,
        )
        duration_timeout = kwargs.get(
            'duration_timeout',
            activity.task_schedule_to_close_timeout,
        )
        schedule_timeout = kwargs.get(
            'schedule_timeout',
            activity.task_schedule_to_start_timeout,
        )
        heartbeat_timeout = kwargs.get(
            'heartbeat_timeout',
            activity.task_heartbeat_timeout,
        )
        task_priority = kwargs.get('priority')

        decision = swf.models.decision.ActivityTaskDecision(
            'schedule',
            activity_id=self.id,
            activity_type=model,
            control=None,
            task_list=task_list,
            input=input,
            task_timeout=str(task_timeout) if task_timeout else None,
            duration_timeout=str(duration_timeout) if duration_timeout else None,
            schedule_timeout=str(schedule_timeout) if schedule_timeout else None,
            heartbeat_timeout=str(heartbeat_timeout) if heartbeat_timeout else None,
            task_priority=task_priority,
        )

        return [decision]


class WorkflowTask(task.WorkflowTask, SwfTask):
    """
    WorkflowTask managed on SWF.
    """
    @classmethod
    def from_generic_task(cls, task):
        """
        Casts a generic simpleflow.task.WorkflowTask into a SWF one.
        """
        return cls(task.executor, task.workflow, *task._args, **task._kwargs)

    @property
    def name(self):
        return 'workflow-{}'.format(self.workflow.name)

    @property
    def task_list(self):
        return getattr(self.workflow, 'task_list', None)

    def schedule(self, domain, task_list=None, priority=None):
        """
        Schedule a child workflow.

        :param domain:
        :type domain: swf.models.Domain
        :param task_list:
        :type task_list: Optional[str]
        :param priority: ignored (only there for compatibility reasons with ActivityTask)
        :type priority: Optional[str|int]
        :return:
        :rtype: list[swf.models.decision.Decision]
        """
        workflow = self.workflow
        # FIXME Always involve a GET call to the SWF API which introduces useless
        # latency if the WorkflowType already exists.
        model = swf.models.WorkflowType(
            domain,
            workflow.__module__ + '.' + workflow.__name__,
            version=workflow.version,
        )

        input = {
            'args': self.args,
            'kwargs': self.kwargs,
        }

        get_tag_list = getattr(workflow, 'get_tag_list', None)
        if get_tag_list:
            tag_list = get_tag_list(workflow, *self.args, **self.kwargs)
        else:
            tag_list = getattr(workflow, 'tag_list', None)

        execution_timeout = getattr(workflow, 'execution_timeout', None)
        decision = swf.models.decision.ChildWorkflowExecutionDecision(
            'start',
            workflow_id=self.id,
            workflow_type=model,
            task_list=task_list or self.task_list,
            input=input,
            tag_list=tag_list,
            child_policy=getattr(workflow, 'child_policy', None),
            execution_timeout=str(execution_timeout) if execution_timeout else None,
        )

        return [decision]


class SignalTask(task.SignalTask, SwfTask):
    """
    Signal "task" on SWF.
    """
    @classmethod
    def from_generic_task(cls, a_task, workflow_id, run_id, control, extra_input):
        return cls(a_task.name, workflow_id, run_id, control, extra_input, *a_task.args, **a_task.kwargs)

    def __init__(self, name, workflow_id, run_id, control=None, extra_input=None, *args, **kwargs):
        super(SignalTask, self).__init__(name, *args, **kwargs)
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.control = control
        self.extra_input = extra_input

    @property
    def id(self):
        return self._name

    @property
    def idempotent(self):
        return None

    def __repr__(self):
        return '{}(name={}, workflow_id={}, run_id={}, control={}, args={}, kwargs={})'.format(
            self.__class__.__name__,
            self.name,
            self.workflow_id,
            self.run_id,
            self.control,
            self.args,
            self.kwargs,
        )

    def schedule(self, domain, task_list, priority=None):
        input = {
            'args': self.args,
            'kwargs': self.kwargs,
            '__workflow_id': self.workflow_id,
            '__run_id': self.run_id,
        }
        if self.extra_input:
            input.update(self.extra_input)
        logger.debug(
            'scheduling signal name={name}, workflow_id={workflow_id}, run_id={run_id}, control={control}'.format(
                name=self.name,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
                control=self.control,
            )
        )

        decision = swf.models.decision.ExternalWorkflowExecutionDecision()
        decision.signal(
            signal_name=self.name,
            input=input,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
            control=self.control,
        )

        return [decision]


class MarkerTask(task.MarkerTask, SwfTask):

    idempotent = False

    @classmethod
    def from_generic_task(cls, a_task):
        # type: (task.MarkerTask) -> MarkerTask
        return cls(a_task.name, *a_task.args, **a_task.kwargs)

    def __init__(self, name, details=None):
        super(MarkerTask, self).__init__(name, details)

    @property
    def id(self):
        return self.name

    def schedule(self, *args, **kwargs):
        decision = swf.models.decision.MarkerDecision()
        decision.record(
            self.name,
            self.get_json_details(),
        )
        return [decision]

    def get_json_details(self):
        return json_dumps(self.details) if self.details is not None else None
