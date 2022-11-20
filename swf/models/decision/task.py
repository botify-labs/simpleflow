# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

from simpleflow import format
from swf.models.decision.base import Decision, decision_action


class ActivityTaskDecision(Decision):
    _base_type = "ActivityTask"

    @decision_action
    def request_cancel(self, activity_id):
        """Request activity task cancel decision builder

        :param  activity_id: activity task to be canceled id
        :type   activity_id: str
        """
        self.update_attributes(
            {
                "activityId": activity_id,
            }
        )

    @decision_action
    def schedule(
        self,
        activity_id,
        activity_type,
        control=None,
        heartbeat_timeout=None,
        input=None,
        duration_timeout=None,
        schedule_timeout=None,
        task_timeout=None,
        task_list=None,
        task_priority=None,
    ):
        """Schedule activity task decision builder

        :param  activity_id: activity id of the activity task
        :type   activity_id: String

        :param  activity_type: type of the activity task to schedule
        :type   activity_type: swf.models.activity.ActivityType

        :param  control: data attached to the event that can be used by the decider in subsequent workflow tasks
        :type   control: Optional[dict]

        :param  heartbeat_timeout: Specifies the maximum time before which a worker processing a task of this type must
                report progress
        :type   heartbeat_timeout: String

        :param  input: input provided to the activity task
        :type   input: Optional[dict]

        :param  duration_timeout: Maximum duration for this activity task
        :type   duration_timeout: String

        :param  schedule_timeout: Specifies the maximum duration the activity task can wait to be assigned to a worker
        :type   schedule_timeout: String

        :param  task_timeout: Specifies the maximum duration a worker may take to process this activity task
        :type   task_timeout: String

        :param  task_list: Specifies the name of the task list in which to schedule the activity task
        :type   :str

        :param  task_priority: Specifies the numeric priority of the task to pass to SWF (defaults to None).
        :type   task_priority: int|String
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
