from __future__ import annotations

from typing import TYPE_CHECKING, Any

from simpleflow import format
from simpleflow.swf.mapper.models.decision.base import Decision, decision_action

if TYPE_CHECKING:
    from simpleflow.swf.mapper.models.activity import ActivityType


class ActivityTaskDecision(Decision):
    _base_type = "ActivityTask"

    @decision_action
    def request_cancel(self, activity_id: str) -> None:
        """Request activity task cancel decision builder

        :param  activity_id: activity task to be canceled id
        """
        self.update_attributes(
            {
                "activityId": activity_id,
            }
        )

    @decision_action
    def schedule(
        self,
        activity_id: str,
        activity_type: ActivityType,
        control: dict[str, Any] | None = None,
        heartbeat_timeout: str | None = None,
        input: dict[str, Any] | None = None,
        duration_timeout: str | None = None,
        schedule_timeout: str | None = None,
        task_timeout: str | None = None,
        task_list: str | None = None,
        task_priority: str | int | None = None,
    ):
        """Schedule activity task decision builder

        :param  activity_id: activity id of the activity task
        :param  activity_type: type of the activity task to schedule
        :param  control: data attached to the event that can be used by the decider in subsequent workflow tasks
        :param  heartbeat_timeout: Specifies the maximum time before which a worker processing a task of this type must
                report progress
        :param  input: input provided to the activity task
        :param  duration_timeout: Maximum duration for this activity task
        :param  schedule_timeout: Specifies the maximum duration the activity task can wait to be assigned to a worker
        :param  task_timeout: Specifies the maximum duration a worker may take to process this activity task
        :param  task_list: Specifies the name of the task list in which to schedule the activity task
        :param  task_priority: Specifies the numeric priority of the task to pass to SWF (defaults to None).
        """
        if input is not None:
            input = format.input(input)
        if control is not None:
            control = format.control(control)

        if task_priority is not None:
            # NB: here we call int() so we raise early if a wrong task priority
            # is passed to this function.
            task_priority = str(int(task_priority))

        self.update_attributes(
            {
                "activityId": activity_id,
                "activityType": {
                    "name": activity_type.name,
                    "version": activity_type.version,
                },
                "control": control,
                "heartbeatTimeout": heartbeat_timeout,
                "input": input,
                "scheduleToCloseTimeout": duration_timeout,
                "scheduleToStartTimeout": schedule_timeout,
                "startToCloseTimeout": task_timeout,
                "taskList": {"name": task_list},
                "taskPriority": task_priority,
            }
        )
