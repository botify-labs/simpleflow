import logging

import swf.models
import swf.models.decision

from simpleflow import task

logger = logging.getLogger(__name__)


class ActivityTask(task.ActivityTask):
    """
    Activity task managed on SWF.
    """

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
        :type kwargs: dict
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
        )

        return [decision]


class WorkflowTask(task.WorkflowTask):
    """
    WorkflowTask managed on SWF.
    """

    @property
    def name(self):
        return 'workflow-{}'.format(self.workflow.name)

    @property
    def task_list(self):
        return getattr(self.workflow, 'task_list', None)

    def schedule(self, domain, task_list=None):
        """
        Schedule a child workflow.

        :param domain:
        :type domain: swf.models.Domain
        :param task_list:
        :type task_list: Optional[str]
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
