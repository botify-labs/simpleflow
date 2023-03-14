from __future__ import annotations

import unittest
from unittest import mock

from sure import expect

from simpleflow import activity, format, futures
from simpleflow.swf.executor import Executor
from swf.models.history import builder
from swf.responses import Response
from tests.data.activities import increment
from tests.data.constants import DOMAIN
from tests.data.workflows import BaseTestWorkflow
from tests.utils import MockSWFTestCase


@activity.with_attributes(task_priority=32)
def increment_high_priority(self, x):
    return x + 1


class ExampleWorkflow(BaseTestWorkflow):
    """
    Example workflow definition used in tests below.
    """

    @property
    def task_priority(self):
        """
        Sets a default task priority as a dynamic value. We could also have used
        task_priority = <num> on the class directly.
        """
        return 12

    def run(self):
        a = self.submit(increment, 3)
        b = self.submit(increment, 3, __priority=5)
        c = self.submit(increment, 3, __priority=None)
        d = self.submit(increment_high_priority, 3)
        e = self.submit(increment_high_priority, 3, __priority=30)
        futures.wait(a, b, c, d, e)


class TestSimpleflowSwfExecutor(MockSWFTestCase):
    def test_submit_resolves_priority(self):
        self.start_workflow_execution()
        decisions = self.build_decisions(ExampleWorkflow).decisions

        expect(decisions).to.have.length_of(5)

        def get_task_priority(decision):
            return decision["scheduleActivityTaskDecisionAttributes"].get("taskPriority")

        # default priority for the whole workflow
        expect(get_task_priority(decisions[0])).to.equal("12")

        # priority passed explicitly
        expect(get_task_priority(decisions[1])).to.equal("5")

        # priority == None
        expect(get_task_priority(decisions[2])).to.be.none

        # priority set at decorator level
        expect(get_task_priority(decisions[3])).to.equal("32")

        # priority set at decorator level but overridden in self.submit()
        expect(get_task_priority(decisions[4])).to.equal("30")


class TestCaseNotNeedingDomain(unittest.TestCase):
    def test_get_event_details(self):
        history = builder.History(ExampleWorkflow, input={})
        signal_input = {"x": 42, "foo": "bar", "__propagate": False}
        marker_details = {"baz": "bae"}
        history.add_signal("a_signal", signal_input)
        history.add_marker("a_marker", marker_details)
        history.add_timer_started("a_timer", 1, decision_id=2)
        history.add_timer_fired("a_timer")

        executor = Executor(DOMAIN, ExampleWorkflow)
        executor.replay(Response(history=history, execution=None))

        details = executor.get_event_details("signal", "a_signal")
        del details["timestamp"]
        expect(details).to.equal(
            {
                "type": "signal",
                "state": "signaled",
                "name": "a_signal",
                "input": signal_input,
                "event_id": 4,
                "external_initiated_event_id": 0,
                "external_run_id": None,
                "external_workflow_id": None,
            }
        )

        details = executor.get_event_details("signal", "another_signal")
        expect(details).to.be.none

        details = executor.get_event_details("marker", "a_marker")
        del details["timestamp"]
        expect(details).to.equal(
            {
                "type": "marker",
                "state": "recorded",
                "name": "a_marker",
                "details": marker_details,
                "event_id": 5,
            }
        )
        details = executor.get_event_details("marker", "another_marker")
        expect(details).to.be.none

        details = executor.get_event_details("timer", "a_timer")
        del details["started_event_timestamp"]
        del details["fired_event_timestamp"]
        expect(details).to.equal(
            {
                "type": "timer",
                "state": "fired",
                "id": "a_timer",
                "decision_task_completed_event_id": 2,
                "start_to_fire_timeout": 1,
                "started_event_id": 6,
                "fired_event_id": 7,
                "control": None,
            }
        )
        details = executor.get_event_details("timer", "another_timer")
        expect(details).to.be.none


@activity.with_attributes(raises_on_failure=True)
def print_me_n_times(s, n, raises=False):
    if raises:
        raise ValueError(f"Number: {s * n}")
    return s * n


class ExampleJumboWorkflow(BaseTestWorkflow):
    """
    Example workflow definition used in tests below.
    """

    def run(self, s, n, raises=False):
        a = self.submit(print_me_n_times, s, n, raises=raises)
        futures.wait(a)
        return a.result


class TestSimpleflowSwfExecutorWithJumboFields(MockSWFTestCase):
    @mock.patch.dict("os.environ", {"SIMPLEFLOW_JUMBO_FIELDS_BUCKET": "jumbo-bucket"})
    def test_jumbo_fields_are_replaced_correctly(self):
        # prepare
        self.register_activity_type("tests.test_simpleflow.swf.test_executor.print_me_n_times", "default")

        # start execution
        self.start_workflow_execution(input='{"args": ["012345679", 10000]}')

        # decider part
        result = self.build_decisions(ExampleJumboWorkflow)
        assert len(result.decisions) == 1
        self.take_decisions(result.decisions, result.execution_context)

        # worker part
        self.process_activity_task()

        # now check the history
        events = self.get_workflow_execution_history()["events"]

        activity_result_evt = events[-2]
        assert activity_result_evt["eventType"] == "ActivityTaskCompleted"
        result = activity_result_evt["activityTaskCompletedEventAttributes"]["result"]

        expect(result).to.match(r"^simpleflow\+s3://jumbo-bucket/[a-z0-9-]+ 90002$")

    @mock.patch.dict("os.environ", {"SIMPLEFLOW_JUMBO_FIELDS_BUCKET": "jumbo-bucket"})
    def test_jumbo_fields_in_task_failed_is_decoded(self):
        # prepare execution
        self.register_activity_type("tests.test_simpleflow.swf.test_executor.print_me_n_times", "default")

        # start execution
        self.start_workflow_execution(
            input='{"args": ["012345679", 10000], "kwargs": {"raises": true}}',
        )

        # decider part
        result = self.build_decisions(ExampleJumboWorkflow)
        assert len(result.decisions) == 1
        self.take_decisions(result.decisions, result.execution_context)

        # worker part
        self.process_activity_task()

        # now check the history
        events = self.get_workflow_execution_history()["events"]

        activity_result_evt = events[-2]
        assert activity_result_evt["eventType"] == "ActivityTaskFailed"
        attrs = activity_result_evt["activityTaskFailedEventAttributes"]
        expect(attrs["reason"]).to.match(r"simpleflow\+s3://jumbo-bucket/[a-z0-9-]+ 9\d{4}")
        expect(attrs["details"]).to.match(r"simpleflow\+s3://jumbo-bucket/[a-z0-9-]+ 9\d{4}")
        details = format.decode(attrs["details"])
        expect(details["error"]).to.equal("ValueError")
        expect(len(details["message"])).to.be.greater_than(9 * 10000)

        # decide again (should lead to workflow failure)
        result = self.build_decisions(ExampleJumboWorkflow)
        assert len(result.decisions) == 1
        assert result.decisions[0]["decisionType"] == "FailWorkflowExecution"
        self.take_decisions(result.decisions, result.execution_context)

        # now check history again
        events = self.get_workflow_execution_history()["events"]

        event = events[-1]
        assert event["eventType"] == "WorkflowExecutionFailed"
        attrs = event["workflowExecutionFailedEventAttributes"]

        details = format.decode(attrs["details"], use_proxy=False)
        expect(details).to.be.a("dict")
        expect(details["message"]).to.match(r"^Number: 012345.*")

        reason = format.decode(attrs["reason"], use_proxy=False)
        expect(reason).to.match(
            r"^Workflow execution error in activity-tests.test_simpleflow.swf."
            r'test_executor.print_me_n_times: "ValueError: Number: 012345679\d+"$'
        )
