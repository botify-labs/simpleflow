from __future__ import annotations

from simpleflow.swf.executor import Executor
from swf.models import Domain
from swf.models.history import builder
from swf.responses import Response


class TestWorkflowMixin:
    def build_history(self, workflow_input):
        domain = Domain("TestDomain")
        self.executor = Executor(domain, self.WORKFLOW)
        self.history = builder.History(self.WORKFLOW, input=workflow_input)

    def replay(self):
        decisions = self.executor.replay(Response(history=self.history, execution=None), decref_workflow=False)
        return decisions.decisions

    def check_task_scheduled_decision(self, decision, task):
        """
        Asserts that *decision* schedules *task*.
        """
        assert decision["decisionType"] == "ScheduleActivityTask"

        attributes = decision["scheduleActivityTaskDecisionAttributes"]
        assert attributes["activityType"]["name"] == task.name

    def add_activity_task_from_decision(self, decision, activity, result=None, last_state="completed"):
        attributes = decision["scheduleActivityTaskDecisionAttributes"]
        decision_id = self.history.last_id
        activity_id = attributes["activityId"]
        activity_input = attributes["input"]
        (
            self.history.add_activity_task(
                activity,
                decision_id=decision_id,
                activity_id=activity_id,
                last_state=last_state,
                input=activity_input,
                result=result,
            )
        )
