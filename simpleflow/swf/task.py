import swf.models

from simpleflow import task


class ActivityTask(task.ActivityTask):
    def schedule(self, domain):
        activity = self.activity
        # Always involve a GET call to the SWF API which introduces useless
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

        decision = swf.models.decision.ActivityTaskDecision(
            'schedule',
            activity_id=self.id,
            activity_type=model,
            control=None,
            task_list=activity.task_list,
            input=input,
            task_timeout=str(activity.task_start_to_close_timeout),
            duration_timeout=str(activity.task_schedule_to_close_timeout),
            schedule_timeout=str(activity.task_schedule_to_start_timeout),
            heartbeat_timeout=str(activity.task_heartbeat_timeout))

        return [decision]


class WorkflowTask(task.WorkflowTask):
    def schedule(self, domain):
        workflow = self.workflow
        # Always involve a GET call to the SWF API which introduces useless
        # latency if the WorkflowType already exists.
        model = swf.models.WorkflowType(
            domain,
            workflow.name,
            version=workflow.version,
        )

        input = {
            'args': self.args,
            'kwargs': self.kwargs,
        }

        decision = swf.models.decision.ChildWorkflowExecutionDecision(
            'start',
            workflow_id=self.id,
            workflow_type=model,
            task_list=workflow.task_list,
            input=input,
            tag_list=workflow.tag_list,
            child_policy=workflow.child_policy,
            execution_timeout=str(workflow.execution_timeout))

        return [decision]
