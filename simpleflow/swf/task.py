import logging

import swf.models
import swf.models.decision
from simpleflow import task
from simpleflow.utils import json_dumps

logger = logging.getLogger(__name__)


class SwfTask(object):
    """
    simpleflow.swf task; useful for type checking.
    """
    pass


class ActivityTask(task.ActivityTask, SwfTask):
    """
    Activity task managed on SWF.
    """
    cached_models = {}

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
        model = self.get_activity_type(
            domain,
            activity.name,
            activity.version
        )

        input = self.get_input()

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

    def get_input(self):
        input = {
            'args': self.args,
            'kwargs': self.kwargs,
        }
        return input

    @classmethod
    def get_activity_type(cls, domain, name, version):
        # type: (swf.models.Domain, str, str) -> swf.models.ActivityType
        """
        Cache known ActivityType's to remove useless latency.
        :param domain:
        :type domain:
        :param name:
        :type name:
        :param version:
        :type version:
        :return:
        :rtype:
        """
        key = (domain.name, name, version)
        if key not in cls.cached_models:
            cls.cached_models[key] = swf.models.ActivityType(
                domain,
                name,
                version=version,
            )
        return cls.cached_models[key]


class NonPythonicActivityTask(ActivityTask):
    """
    ActivityTask that pass raw kwargs or args as input, without "args" and "kwargs" subkeys.
    """

    def __init__(self, activity, *args, **kwargs):
        if args and kwargs:
            raise ValueError("This task type doesn't support both *args and **kwargs")
        super(ActivityTask, self).__init__(activity, *args, **kwargs)

    def get_input(self):
        return self.kwargs or self.args


class WorkflowTask(task.WorkflowTask, SwfTask):
    """
    WorkflowTask managed on SWF.
    """
    cached_models = {}

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

    def schedule(self, domain, task_list=None, **kwargs):
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
        model = self.get_workflow_type(
            domain,
            workflow.__module__ + '.' + workflow.__name__,
            workflow.version
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

    @classmethod
    def get_workflow_type(cls, domain, name, version):
        # type: (swf.models.Domain, str, str) -> swf.models.WorkflowType
        """
        Cache known WorkflowType's to remove useless latency.
        :param domain:
        :type domain:
        :param name:
        :type name:
        :param version:
        :type version:
        :return:
        :rtype:
        """
        key = (domain.name, name, version)
        if key not in cls.cached_models:
            cls.cached_models[key] = swf.models.WorkflowType(
                domain,
                name,
                version=version,
            )
        return cls.cached_models[key]


class SignalTask(task.SignalTask, SwfTask):
    """
    Signal "task" on SWF.
    """
    idempotent = True

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

    def schedule(self, *args, **kwargs):
        input = {
            'args': self.args,
            'kwargs': self.kwargs,
        }
        if self.extra_input:
            input.update(self.extra_input)
        logger.debug(
            'scheduling signal name={name}, workflow_id={workflow_id}, run_id={run_id}, control={control}, '
            'extra_input={extra_input}'.format(
                name=self.name,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
                control=self.control,
                extra_input=self.extra_input,
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
    idempotent = True

    @classmethod
    def from_generic_task(cls, a_task):
        # type: (task.MarkerTask) -> MarkerTask
        return cls(a_task.name, *a_task.args, **a_task.kwargs)

    def __init__(self, name, details=None):
        super(MarkerTask, self).__init__(name, details)
        self.id = None

    def schedule(self, *args, **kwargs):
        decision = swf.models.decision.MarkerDecision()
        decision.record(
            self.name,
            self.get_json_details(),
        )
        return [decision]

    def get_json_details(self):
        return json_dumps(self.details) if self.details is not None else None


class TimerTask(task.TimerTask, SwfTask):
    idempotent = True

    @classmethod
    def from_generic_task(cls, a_task):
        # type: (task.TimerTask) -> TimerTask
        return cls(a_task.timer_id, a_task.timeout, a_task.control)

    def __init__(self, timer_id, timeout, control):
        super(TimerTask, self).__init__(timer_id, timeout, control)

    def schedule(self, *args, **kwargs):
        decision = swf.models.decision.TimerDecision(
            'start',
            id=self.timer_id,
            start_to_fire_timeout=str(self.timeout),
            control=json_dumps(self.control) if self.control is not None else None,
        )
        return [decision]


class CancelTimerTask(task.CancelTimerTask, SwfTask):
    idempotent = True

    @classmethod
    def from_generic_task(cls, a_task):
        # type: (task.CancelTimerTask) -> CancelTimerTask
        return cls(a_task.timer_id)

    def __init__(self, timer_id):
        super(CancelTimerTask, self).__init__(timer_id)

    def schedule(self, *args, **kwargs):
        decision = swf.models.decision.TimerDecision(
            'cancel',
            id=self.timer_id,
        )
        return [decision]
