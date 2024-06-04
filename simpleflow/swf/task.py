from __future__ import annotations

import typing
from typing import Any, ClassVar

import simpleflow.swf.mapper.models
import simpleflow.swf.mapper.models.decision
from simpleflow import logger, settings, task
from simpleflow.swf.utils import set_workflow_class_name
from simpleflow.workflow import Workflow

if typing.TYPE_CHECKING:
    if hasattr(typing, "Self"):
        Self = typing.Self
    else:
        from typing_extensions import Self

    from simpleflow.activity import Activity
    from simpleflow.swf.executor import Executor


class SwfTask:
    """
    simpleflow.swf task; useful for type checking.
    """

    @property
    def payload(self):
        raise NotImplementedError


class ActivityTask(task.ActivityTask, SwfTask):
    """
    Activity task managed on SWF.
    """

    cached_models: ClassVar[dict[tuple[str, str, str], simpleflow.swf.mapper.models.ActivityType]] = {}

    @classmethod
    def from_generic_task(cls, task: task.ActivityTask) -> Self:
        """
        Casts a generic simpleflow.task.ActivityTask into a SWF one.
        """
        return cls(task.activity, *task._args, **task._kwargs)

    @property
    def payload(self):
        return self.activity

    @property
    def task_list(self):
        return self.activity.task_list

    def schedule(
        self,
        domain: simpleflow.swf.mapper.models.Domain,
        task_list: str | None = None,
        **kwargs,
    ) -> list[simpleflow.swf.mapper.models.decision.Decision]:
        """
        Schedule an activity.
        """
        activity = self.activity
        model = self.get_activity_type(domain, activity.name, activity.version)

        input = self.get_input()

        if task_list is None:
            task_list = activity.task_list
        task_timeout = kwargs.get(
            "task_timeout",
            activity.task_start_to_close_timeout,
        )
        duration_timeout = kwargs.get(
            "duration_timeout",
            activity.task_schedule_to_close_timeout,
        )
        schedule_timeout = kwargs.get(
            "schedule_timeout",
            activity.task_schedule_to_start_timeout,
        )
        heartbeat_timeout = kwargs.get(
            "heartbeat_timeout",
            activity.task_heartbeat_timeout,
        )
        task_priority = kwargs.get("priority")
        control = kwargs.get("control")

        meta = activity.meta
        if meta:
            input["meta"] = meta

        decision = simpleflow.swf.mapper.models.decision.ActivityTaskDecision(
            "schedule",
            activity_id=self.id,
            activity_type=model,
            control=control,
            task_list=task_list,
            input=input,
            task_timeout=str(task_timeout) if task_timeout else None,
            duration_timeout=str(duration_timeout) if duration_timeout else None,
            schedule_timeout=str(schedule_timeout) if schedule_timeout else None,
            heartbeat_timeout=str(heartbeat_timeout) if heartbeat_timeout else None,
            task_priority=task_priority,
        )

        return [decision]

    def get_input(self) -> dict[str, Any] | list[Any]:
        input = {
            "args": self.args,
            "kwargs": self.kwargs,
        }
        return input

    @classmethod
    def is_known_activity_type(cls, domain: simpleflow.swf.mapper.models.Domain, name: str, version: str) -> bool:
        key = (domain.name, name, version)
        return key in cls.cached_models

    @classmethod
    def get_activity_type(
        cls, domain: simpleflow.swf.mapper.models.Domain, name: str, version: str
    ) -> simpleflow.swf.mapper.models.ActivityType:
        """
        Cache known ActivityType's to remove useless latency.
        """
        key = (domain.name, name, version)
        if key not in cls.cached_models:
            cls.cached_models[key] = simpleflow.swf.mapper.models.ActivityType(
                domain,
                name,
                version=version,
            )
        return cls.cached_models[key]


class NonPythonicActivityTask(ActivityTask):
    """
    ActivityTask that pass raw kwargs or args as input, without "args" and "kwargs" subkeys.
    """

    def __init__(self, activity: Activity, *args, **kwargs) -> None:
        if args and kwargs:
            raise ValueError("This task type doesn't support both *args and **kwargs")
        super(ActivityTask, self).__init__(activity, *args, **kwargs)

    def get_input(self) -> dict[str, Any] | list[Any]:
        return self.kwargs or self.args


class WorkflowTask(task.WorkflowTask, SwfTask):
    """
    WorkflowTask managed on SWF.
    """

    cached_models: ClassVar[dict[tuple[str, str, str], simpleflow.swf.mapper.models.WorkflowType]] = {}

    @classmethod
    def from_generic_task(cls, task: task.WorkflowTask) -> Self:
        """
        Casts a generic simpleflow.task.WorkflowTask into a SWF one.
        """
        return cls(task.executor, task.workflow, *task._args, **task._kwargs)

    @property
    def name(self):
        return f"workflow-{self.workflow.name}"

    @property
    def payload(self):
        return self.workflow

    @property
    def task_list(self) -> str | None:
        get_task_list = getattr(self.workflow, "get_task_list", None)
        if get_task_list:
            return get_task_list(self.workflow, *self.args, **self.kwargs)

        return getattr(self.workflow, "task_list", None)

    @property
    def tag_list(self) -> list[str] | None:
        get_tag_list = getattr(self.workflow, "get_tag_list", None)
        if get_tag_list:
            return get_tag_list(self.workflow, *self.args, **self.kwargs)

        return getattr(self.workflow, "tag_list", None)

    def schedule(
        self,
        domain: simpleflow.swf.mapper.models.Domain,
        task_list: str | None = None,
        executor: Executor | None = None,
        **kwargs,
    ) -> list[simpleflow.swf.mapper.models.decision.Decision]:
        """
        Schedule a child workflow.
        """
        workflow = self.workflow
        model = self.get_workflow_type(domain, workflow.__module__ + "." + workflow.__name__, workflow.version)

        input = self.get_input()
        control = kwargs.get("control")

        tag_list = self.tag_list
        if tag_list == Workflow.INHERIT_TAG_LIST:
            tag_list = executor.get_run_context()["tag_list"]  # FIXME what about self.executor?

        execution_timeout = getattr(workflow, "execution_timeout", None)
        decision = simpleflow.swf.mapper.models.decision.ChildWorkflowExecutionDecision(
            "start",
            workflow_id=self.id,
            workflow_type=model,
            task_list=task_list or self.task_list,
            control=control,
            input=input,
            tag_list=tag_list,
            child_policy=getattr(workflow, "child_policy", None),
            execution_timeout=str(execution_timeout) if execution_timeout else None,
        )

        return [decision]

    def get_input(self) -> dict[str, Any]:
        input = {
            "args": self.args,
            "kwargs": self.kwargs,
        }
        return input

    @classmethod
    def get_workflow_type(
        cls, domain: simpleflow.swf.mapper.models.Domain, name: str, version: str
    ) -> simpleflow.swf.mapper.models.WorkflowType:
        """
        Cache known WorkflowType's to remove useless latency.
        """
        key = (domain.name, name, version)
        if key not in cls.cached_models:
            cls.cached_models[key] = simpleflow.swf.mapper.models.WorkflowType(
                domain,
                name,
                version=version,
            )
        return cls.cached_models[key]


class ContinueAsNewWorkflowTask(WorkflowTask):
    def schedule(
        self,
        domain: simpleflow.swf.mapper.models.Domain,
        task_list: str | None = None,
        executor: Executor | None = None,
        **kwargs,
    ) -> list[simpleflow.swf.mapper.models.decision.Decision]:
        workflow = self.workflow
        tag_list = self.tag_list
        if tag_list == Workflow.INHERIT_TAG_LIST:
            tag_list = executor.get_run_context()["tag_list"]
        execution_timeout = getattr(workflow, "execution_timeout", None)

        decision = simpleflow.swf.mapper.models.decision.WorkflowExecutionDecision()
        input = self.get_input()
        set_workflow_class_name(input, workflow)
        logger.debug(f"ContinueAsNewWorkflowTask: input={input}")
        decision.continue_as_new(
            child_policy=getattr(workflow, "child_policy", None),
            execution_timeout=str(execution_timeout) if execution_timeout else None,
            task_timeout=settings.WORKFLOW_DEFAULT_DECISION_TASK_TIMEOUT,
            input=input,
            tag_list=tag_list,
            task_list=task_list or self.task_list,
            workflow_type_version=getattr(workflow, "version", None),
        )
        return [decision]


class SignalTask(task.SignalTask, SwfTask):
    """
    Signal "task" on SWF.
    """

    idempotent = True

    @classmethod
    def from_generic_task(
        cls,
        a_task: task.SignalTask,
        workflow_id: str,
        run_id: str,
        control: dict[str, Any] | str | None = None,
        extra_input: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            a_task.name,
            workflow_id,
            run_id,
            control,
            extra_input,
            *a_task.args,
            **a_task.kwargs,
        )

    def __init__(
        self,
        name: str,
        workflow_id: str,
        run_id: str,
        control: dict[str, Any] | str | None = None,
        extra_input: dict[str, Any] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(name, *args, **kwargs)
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.control = control
        self.extra_input = extra_input

    @property
    def id(self):
        return self._name

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(name={self.name},"
            f" workflow_id={self.workflow_id}, run_id={self.run_id}, control={self.control},"
            f" args={self.args}, kwargs={self.kwargs})"
        )

    def schedule(self, *args, **kwargs) -> list[simpleflow.swf.mapper.models.decision.Decision]:
        input: dict[str, Any] = {
            "args": self.args,
            "kwargs": self.kwargs,
        }
        if self.extra_input:
            input.update(self.extra_input)
        logger.debug(
            f"scheduling signal name={self.name}, workflow_id={self.workflow_id}, run_id={self.run_id},"
            f" control={self.control}, extra_input={self.extra_input}"
        )

        decision = simpleflow.swf.mapper.models.decision.ExternalWorkflowExecutionDecision()
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
    def from_generic_task(cls, a_task: task.MarkerTask) -> Self:
        return cls(a_task.name, *a_task.args, **a_task.kwargs)

    def __init__(self, name: str, details: str | None = None) -> None:
        super().__init__(name, details)
        self.id = None

    def schedule(self, *args, **kwargs) -> list[simpleflow.swf.mapper.models.decision.Decision]:
        decision = simpleflow.swf.mapper.models.decision.MarkerDecision()
        decision.record(
            self.name,
            self.details,
        )
        return [decision]


class TimerTask(task.TimerTask, SwfTask):
    idempotent = True

    @classmethod
    def from_generic_task(cls, a_task: task.TimerTask) -> Self:
        return cls(a_task.timer_id, a_task.timeout, a_task.control)

    def __init__(self, timer_id: str, timeout: int | str, control: dict[str, Any] | None) -> None:
        super().__init__(timer_id, timeout, control)

    def schedule(self, *args, **kwargs) -> list[simpleflow.swf.mapper.models.decision.Decision]:
        decision = simpleflow.swf.mapper.models.decision.TimerDecision(
            "start",
            id=self.timer_id,
            start_to_fire_timeout=str(self.timeout),
            control=self.control,
        )
        return [decision]


class CancelTimerTask(task.CancelTimerTask, SwfTask):
    idempotent = True

    @classmethod
    def from_generic_task(cls, a_task: task.CancelTimerTask) -> Self:
        return cls(a_task.timer_id)

    def __init__(self, timer_id: str) -> None:
        super().__init__(timer_id)

    def schedule(self, *args, **kwargs) -> list[simpleflow.swf.mapper.models.decision.Decision]:
        decision = simpleflow.swf.mapper.models.decision.TimerDecision(
            "cancel",
            id=self.timer_id,
        )
        return [decision]
