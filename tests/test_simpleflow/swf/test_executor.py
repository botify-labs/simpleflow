from __future__ import annotations

import re
import unittest
from unittest import mock

from simpleflow import activity, exceptions, format, futures
from simpleflow.history import History
from simpleflow.swf.executor import Executor
from simpleflow.swf.mapper.models.history import builder
from simpleflow.swf.mapper.responses import Response
from tests.data.activities import increment, raise_on_failure
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

        assert len(decisions) == 5

        def get_task_priority(decision):
            return decision["scheduleActivityTaskDecisionAttributes"].get("taskPriority")

        # default priority for the whole workflow
        assert get_task_priority(decisions[0]) == "12"

        # priority passed explicitly
        assert get_task_priority(decisions[1]) == "5"

        # priority == None
        assert get_task_priority(decisions[2]) is None

        # priority set at decorator level
        assert get_task_priority(decisions[3]) == "32"

        # priority set at decorator level but overridden in self.submit()
        assert get_task_priority(decisions[4]) == "30"


class StartTimerWorkflow(BaseTestWorkflow):
    future = None

    def run(self):
        type(self).future = self.submit(self.start_timer("a_timer", 10))


class CancelTimerWorkflow(BaseTestWorkflow):
    future = None

    def run(self):
        type(self).future = self.submit(self.cancel_timer("a_timer"))


class RetryLaterWorkflow(BaseTestWorkflow):
    def run(self):
        futures.wait(self.submit(raise_on_failure))

    def on_task_failure(self, failure_context):
        return failure_context.decide_retry(retry_wait_timeout=30)


def replay_decisions(workflow, history):
    return Executor(DOMAIN, workflow).replay(Response(history=history, execution=None)).decisions


RETRIED_ACTIVITY_ID = "activity-tests.data.activities.raise_on_failure-1"


def history_with_a_refused_retry_timer(cause, earlier_retries):
    history = builder.History(RetryLaterWorkflow, input={})
    history.add_decision_task_completed()
    for attempt in range(earlier_retries + 1):
        history.add_activity_task(
            raise_on_failure, decision_id=history.last_id, last_state="failed", activity_id=RETRIED_ACTIVITY_ID
        )
        history.add_decision_task()
        (retry_timer,) = replay_decisions(RetryLaterWorkflow, history)
        assert retry_timer["decisionType"] == "StartTimer"
        timer_id = retry_timer["startTimerDecisionAttributes"]["timerId"]
        if attempt < earlier_retries:
            history.add_timer_started(timer_id, 30, decision_id=history.last_id)
            history.add_timer_fired(timer_id)
            history.add_decision_task()
    history.add_timer_start_failed(timer_id, cause, decision_id=history.last_id)
    history.add_decision_task_scheduled()
    history.add_decision_task_started()
    return history, retry_timer


class TestCaseNotNeedingDomain(unittest.TestCase):
    def test_a_retry_timer_refused_for_rate_is_started_again(self):
        for earlier_retries in (0, 1):
            with self.subTest(earlier_retries=earlier_retries):
                history, retry_timer = history_with_a_refused_retry_timer(
                    "TIMER_CREATION_RATE_EXCEEDED", earlier_retries
                )

                assert replay_decisions(RetryLaterWorkflow, history) == [retry_timer]

                timer_id = retry_timer["startTimerDecisionAttributes"]["timerId"]
                history.add_decision_task_completed()
                history.add_timer_started(timer_id, 30, decision_id=history.last_id)
                history.add_timer_fired(timer_id)
                history.add_decision_task_scheduled()
                history.add_decision_task_started()

                (decision,) = replay_decisions(RetryLaterWorkflow, history)
                assert decision["decisionType"] == "ScheduleActivityTask"
                assert decision["scheduleActivityTaskDecisionAttributes"]["activityId"] == RETRIED_ACTIVITY_ID

    def test_a_retry_timer_refused_for_good_fails_the_task(self):
        for earlier_retries in (0, 1):
            with self.subTest(earlier_retries=earlier_retries):
                history, _ = history_with_a_refused_retry_timer("OPERATION_NOT_PERMITTED", earlier_retries)

                (decision,) = replay_decisions(RetryLaterWorkflow, history)

                assert decision["decisionType"] == "FailWorkflowExecution"
                reason = decision["failWorkflowExecutionDecisionAttributes"]["reason"]
                assert reason == (
                    "Workflow execution error in activity-tests.data.activities.raise_on_failure:"
                    ' "OPERATION_NOT_PERMITTED"'
                )

    def test_a_refused_timer_start_replays_as_a_failed_future(self):
        history = builder.History(StartTimerWorkflow, input={})
        history.add_timer_start_failed("a_timer", "TIMER_CREATION_RATE_EXCEEDED", decision_id=2)

        StartTimerWorkflow.future = None
        executor = Executor(DOMAIN, StartTimerWorkflow)
        executor.replay(Response(history=history, execution=None))

        future = StartTimerWorkflow.future
        assert future is not None
        assert future.finished
        assert isinstance(future.exception, exceptions.TaskFailed)
        assert future.exception.name == "a_timer"
        assert future.exception.reason == "TIMER_CREATION_RATE_EXCEEDED"

    def test_a_failed_timer_cancel_replays_as_a_failed_future(self):
        history = builder.History(CancelTimerWorkflow, input={})
        history.add_timer_started("a_timer", 10, decision_id=2)
        history.add_timer_cancel_failed("a_timer", "TIMER_ID_UNKNOWN", decision_id=5)

        CancelTimerWorkflow.future = None
        executor = Executor(DOMAIN, CancelTimerWorkflow)
        executor.replay(Response(history=history, execution=None))

        future = CancelTimerWorkflow.future
        assert future is not None
        assert future.finished
        assert isinstance(future.exception, exceptions.TaskFailed)
        assert future.exception.name == "a_timer"
        assert future.exception.reason == "TIMER_ID_UNKNOWN"

    def test_a_timer_cancel_refused_after_the_fire_keeps_the_timer_fired(self):
        history = builder.History(CancelTimerWorkflow, input={})
        history.add_timer_started("a_timer", 10, decision_id=2)
        history.add_timer_fired("a_timer")
        history.add_decision_task()
        history.add_timer_cancel_failed("a_timer", "TIMER_ID_UNKNOWN", decision_id=history.last_id)
        history.add_decision_task_scheduled()
        history.add_decision_task_started()

        for workflow in (StartTimerWorkflow, CancelTimerWorkflow):
            with self.subTest(workflow=workflow.__name__):
                workflow.future = None
                executor = Executor(DOMAIN, workflow)
                decisions = executor.replay(Response(history=history, execution=None)).decisions

                assert [d["decisionType"] for d in decisions] == ["CompleteWorkflowExecution"]
                future = workflow.future
                assert future.finished
                assert future.exception is None
                timer = executor.get_event_details("timer", "a_timer")
                assert timer["state"] == "fired"
                assert timer["cancel_failed_cause"] == "TIMER_ID_UNKNOWN"
                assert "cause" not in timer

    def test_a_second_timer_cancel_refused_keeps_the_timer_canceled(self):
        history = builder.History(StartTimerWorkflow, input={})
        history.add_timer_started("a_timer", 10, decision_id=2)
        history.add_decision_task()
        history.add_timer_canceled("a_timer", decision_id=history.last_id)
        history.add_timer_cancel_failed("a_timer", "TIMER_ID_UNKNOWN", decision_id=history.last_id - 1)
        history.add_decision_task_scheduled()
        history.add_decision_task_started()

        StartTimerWorkflow.future = None
        executor = Executor(DOMAIN, StartTimerWorkflow)
        executor.replay(Response(history=history, execution=None))

        assert StartTimerWorkflow.future.cancelled
        assert executor.get_event_details("timer", "a_timer")["state"] == "canceled"

    def test_a_timer_cancel_refused_after_a_refused_start_keeps_the_start_failure(self):
        history = builder.History(StartTimerWorkflow, input={})
        history.add_timer_start_failed("a_timer", "OPEN_TIMERS_LIMIT_EXCEEDED", decision_id=2)
        history.add_decision_task()
        history.add_timer_cancel_failed("a_timer", "TIMER_ID_UNKNOWN", decision_id=history.last_id)

        executor = Executor(DOMAIN, StartTimerWorkflow)
        executor.replay(Response(history=history, execution=None))

        timer = executor.get_event_details("timer", "a_timer")
        assert timer["state"] == "start_failed"
        assert timer["cause"] == "OPEN_TIMERS_LIMIT_EXCEEDED"
        assert timer["cancel_failed_cause"] == "TIMER_ID_UNKNOWN"

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
        assert details == {
            "type": "signal",
            "state": "signaled",
            "name": "a_signal",
            "input": signal_input,
            "event_id": 4,
            "external_initiated_event_id": 0,
            "external_run_id": None,
            "external_workflow_id": None,
        }

        details = executor.get_event_details("signal", "another_signal")
        assert details is None

        details = executor.get_event_details("marker", "a_marker")
        del details["timestamp"]
        assert details == {
            "type": "marker",
            "state": "recorded",
            "name": "a_marker",
            "details": marker_details,
            "event_id": 5,
        }
        details = executor.get_event_details("marker", "another_marker")
        assert details is None

        details = executor.get_event_details("timer", "a_timer")
        del details["started_event_timestamp"]
        del details["fired_event_timestamp"]
        assert details == {
            "type": "timer",
            "state": "fired",
            "id": "a_timer",
            "decision_task_completed_event_id": 2,
            "start_to_fire_timeout": 1,
            "started_event_id": 6,
            "fired_event_id": 7,
            "control": None,
        }
        details = executor.get_event_details("timer", "another_timer")
        assert details is None


class SendSignalWorkflow(BaseTestWorkflow):
    future = None

    def run(self):
        type(self).future = self.submit(self.signal("a_signal", workflow_id="target-wf", run_id="target-run"))
        futures.wait(type(self).future)


class SignalItselfWorkflow(BaseTestWorkflow):
    future = None
    target = None

    def run(self):
        type(self).future = self.submit(self.signal("a_signal", **(type(self).target or {})))
        futures.wait(type(self).future)


class PropagateSignalsWorkflow(BaseTestWorkflow):
    def run(self):
        pass


def signal_decisions(decisions):
    return [d for d in decisions if d["decisionType"] == "SignalExternalWorkflowExecution"]


class TestSentSignalReplay(unittest.TestCase):
    def history_with_a_sent_signal(self):
        history = builder.History(SendSignalWorkflow, input={})
        executor = Executor(DOMAIN, SendSignalWorkflow)
        (decision,) = signal_decisions(executor.replay(Response(history=history, execution=None)).decisions)
        attributes = decision["signalExternalWorkflowExecutionDecisionAttributes"]
        history.add_decision_task_completed()
        history.add_signal_execution_initiated(
            attributes["workflowId"],
            attributes["signalName"],
            input=format.decode(attributes["input"]),
            run_id=attributes["runId"],
            decision_id=history.last_id,
        )
        return history, history.last_id

    def replay(self, history):
        history.add_decision_task_scheduled()
        history.add_decision_task_started()
        SendSignalWorkflow.future = None
        executor = Executor(DOMAIN, SendSignalWorkflow)
        decisions = executor.replay(Response(history=history, execution=None)).decisions
        return executor, decisions, SendSignalWorkflow.future

    def test_a_signal_in_flight_is_not_sent_again(self):
        history, _ = self.history_with_a_sent_signal()

        executor, decisions, future = self.replay(history)

        assert [w["state"] for w in executor._history.external_workflows_signaling.values()] == [
            "signal_execution_initiated"
        ]
        assert signal_decisions(decisions) == []
        assert not future.done

    def test_a_refused_signal_is_not_sent_again_and_fails_its_future(self):
        history, initiated_id = self.history_with_a_sent_signal()
        history.add_signal_execution_failed(
            initiated_id, "target-wf", "UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION", run_id="target-run"
        )

        executor, decisions, future = self.replay(history)

        assert [w["state"] for w in executor._history.external_workflows_signaling.values()] == [
            "signal_execution_failed"
        ]
        assert signal_decisions(decisions) == []
        assert future.finished
        assert isinstance(future.exception, exceptions.TaskFailed)
        assert future.exception.name == "a_signal"
        assert future.exception.reason == "UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION"

    def test_a_delivered_signal_finishes_its_future(self):
        history, initiated_id = self.history_with_a_sent_signal()
        history.add_external_workflow_signaled(initiated_id, "target-wf", "target-run")

        _, decisions, future = self.replay(history)

        assert signal_decisions(decisions) == []
        assert future.finished
        assert future.exception is None

    def test_a_rate_limited_signal_is_sent_again(self):
        history, initiated_id = self.history_with_a_sent_signal()
        history.add_signal_execution_failed(
            initiated_id, "target-wf", "SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_RATE_EXCEEDED", run_id="target-run"
        )

        _, decisions, future = self.replay(history)

        (decision,) = signal_decisions(decisions)
        assert decision["signalExternalWorkflowExecutionDecisionAttributes"]["workflowId"] == "target-wf"
        assert not future.done

    def test_a_directed_signal_is_sent_despite_a_received_signal_of_the_same_name(self):
        history = builder.History(SendSignalWorkflow, input={})
        history.add_signal("a_signal", {"args": [], "kwargs": {}})

        _, decisions, future = self.replay(history)

        (decision,) = signal_decisions(decisions)
        assert decision["signalExternalWorkflowExecutionDecisionAttributes"]["workflowId"] == "target-wf"
        assert not future.done

    def test_a_delivered_directed_signal_is_not_sent_again_after_a_received_signal_of_the_same_name(self):
        history, initiated_id = self.history_with_a_sent_signal()
        history.add_external_workflow_signaled(initiated_id, "target-wf", "target-run")
        history.add_signal("a_signal", {"args": [], "kwargs": {}})

        _, decisions, future = self.replay(history)

        assert signal_decisions(decisions) == []
        assert future.finished
        assert future.exception is None

    def replay_signal_to_itself(self, target, own_run_id="own-run", received=True):
        history = builder.History(SignalItselfWorkflow, input={})
        if received:
            history.add_signal("a_signal", {"args": [], "kwargs": {}})
        history.add_decision_task_scheduled()
        history.add_decision_task_started()
        SignalItselfWorkflow.future = None
        SignalItselfWorkflow.target = target
        executor = Executor(DOMAIN, SignalItselfWorkflow)
        executor._run_context = {"workflow_id": "own-wf", "run_id": own_run_id}
        decisions = executor.replay(Response(history=history, execution=None)).decisions
        return signal_decisions(decisions), SignalItselfWorkflow.future

    def test_a_signal_to_itself_is_found_in_the_received_signals(self):
        for target in (None, {"workflow_id": "own-wf"}, {"workflow_id": "own-wf", "run_id": "own-run"}):
            with self.subTest(target=target):
                decisions, future = self.replay_signal_to_itself(target)

                assert decisions == []
                assert future.finished

    def test_a_signal_to_itself_not_yet_received_is_sent(self):
        (decision,) = self.replay_signal_to_itself(None, received=False)[0]

        attributes = decision["signalExternalWorkflowExecutionDecisionAttributes"]
        assert (attributes["workflowId"], attributes["runId"]) == ("own-wf", "own-run")

    def test_a_signal_to_another_run_of_its_workflow_id_is_a_directed_signal(self):
        decisions, future = self.replay_signal_to_itself({"workflow_id": "own-wf", "run_id": "previous-run"})

        (decision,) = decisions
        assert decision["signalExternalWorkflowExecutionDecisionAttributes"]["runId"] == "previous-run"
        assert not future.done

    def history_with_a_propagated_signal_in_flight(self):
        history = builder.History(PropagateSignalsWorkflow, input={})
        history.add_child_workflow(PropagateSignalsWorkflow, last_state="started", workflow_id="child-wf")
        child_run_id = history.events[-1].workflow_execution["runId"]
        history.add_signal("a_signal", {"args": [], "kwargs": {}, "__propagate": True})
        history.add_decision_task_scheduled()
        history.add_decision_task_started()

        (decision,) = self.replay_propagation(history)
        attributes = decision["signalExternalWorkflowExecutionDecisionAttributes"]
        assert (attributes["workflowId"], attributes["runId"]) == ("child-wf", child_run_id)

        history.add_decision_task_completed()
        history.add_signal_execution_initiated(
            "child-wf",
            "a_signal",
            input=format.decode(attributes["input"]),
            run_id=child_run_id,
            decision_id=history.last_id,
        )
        initiated_id = history.last_id
        history.add_decision_task_scheduled()
        history.add_decision_task_started()
        return history, initiated_id, child_run_id, decision

    @staticmethod
    def replay_propagation(history):
        executor = Executor(DOMAIN, PropagateSignalsWorkflow)
        return signal_decisions(executor.replay(Response(history=history, execution=None)).decisions)

    def test_a_propagated_signal_in_flight_is_not_sent_again(self):
        history, _, _, _ = self.history_with_a_propagated_signal_in_flight()

        assert self.replay_propagation(history) == []

    def test_a_refused_propagated_signal_is_sent_again_only_if_rate_limited(self):
        for cause, sent_again in (
            ("UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION", False),
            ("SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_RATE_EXCEEDED", True),
        ):
            with self.subTest(cause=cause):
                history, initiated_id, child_run_id, decision = self.history_with_a_propagated_signal_in_flight()
                history.add_decision_task_completed()
                history.add_signal_execution_failed(initiated_id, "child-wf", cause, run_id=child_run_id)
                history.add_decision_task_scheduled()
                history.add_decision_task_started()

                assert self.replay_propagation(history) == ([decision] if sent_again else [])


class RecordMarkerWorkflow(BaseTestWorkflow):
    future = None

    def run(self):
        type(self).future = self.submit(self.record_marker("a_marker", "some details"))
        futures.wait(type(self).future)


class TestRefusedMarkerReplay(unittest.TestCase):
    def history_with_a_refused_marker(self):
        history = builder.History(RecordMarkerWorkflow, input={})
        (decision,) = self.replay(history)[0]
        assert decision["recordMarkerDecisionAttributes"]["markerName"] == "a_marker"
        history.add_decision_task_completed()
        history.add_marker_record_failed("a_marker", decision_id=history.last_id)
        history.add_decision_task_scheduled()
        history.add_decision_task_started()
        return history

    @staticmethod
    def replay(history):
        RecordMarkerWorkflow.future = None
        decisions = replay_decisions(RecordMarkerWorkflow, history)
        return [d for d in decisions if d["decisionType"] == "RecordMarker"], RecordMarkerWorkflow.future

    def test_a_refused_marker_is_not_recorded_again_and_fails_its_future(self):
        history = self.history_with_a_refused_marker()

        decisions, future = self.replay(history)

        assert decisions == []
        assert future.finished
        assert isinstance(future.exception, exceptions.TaskFailed)
        assert future.exception.name == "a_marker"
        assert future.exception.reason == "OPERATION_NOT_PERMITTED"

    def test_a_recorded_marker_wins_over_a_refusal_whatever_their_order(self):
        for recorded_first in (True, False):
            with self.subTest(recorded_first=recorded_first):
                history = builder.History(RecordMarkerWorkflow, input={})
                history.add_decision_task_completed()
                if recorded_first:
                    history.add_marker("a_marker", "some details")
                history.add_marker_record_failed("a_marker", decision_id=history.last_id)
                if not recorded_first:
                    history.add_marker("a_marker", "some details")
                history.add_decision_task_scheduled()
                history.add_decision_task_started()

                decisions, future = self.replay(history)

                assert decisions == []
                assert future.finished
                assert future.exception is None

    def test_a_refusal_does_not_stand_for_another_marker_name(self):
        history = builder.History(RecordMarkerWorkflow, input={})
        history.add_decision_task_completed()
        history.add_marker_record_failed("another_marker", decision_id=history.last_id)
        history.add_decision_task_scheduled()
        history.add_decision_task_started()

        (decision,) = self.replay(history)[0]

        assert decision["recordMarkerDecisionAttributes"]["markerName"] == "a_marker"


class OneActivityWorkflow(BaseTestWorkflow):
    future = None

    def run(self):
        type(self).future = self.submit(increment, 1)
        futures.wait(type(self).future)


class TestCanceledReplay(unittest.TestCase):
    ACTIVITY_ID = "activity-tests.data.activities.increment-1"

    def test_a_canceled_activity_cancels_its_future(self):
        for started in (True, False):
            with self.subTest(started=started):
                history = builder.History(OneActivityWorkflow, input={})
                history.add_decision_task_completed()
                history.add_activity_task(
                    increment,
                    decision_id=history.last_id,
                    last_state="started" if started else "scheduled",
                    activity_id=self.ACTIVITY_ID,
                )
                scheduled_id = history.last_id - 1 if started else history.last_id
                started_id = history.last_id if started else None
                history.add_activity_task_cancel_requested(self.ACTIVITY_ID)
                history.add_activity_task_canceled(
                    scheduled_id, started=started_id, cancel_requested=history.last_id, details="given up"
                )
                canceled_id = history.last_id
                history.add_decision_task_scheduled()
                history.add_decision_task_started()

                OneActivityWorkflow.future = None
                executor = Executor(DOMAIN, OneActivityWorkflow)
                decisions = executor.replay(Response(history=history, execution=None)).decisions

                assert OneActivityWorkflow.future.cancelled
                assert [d["decisionType"] for d in decisions] == ["CompleteWorkflowExecution"]
                details = executor._history.activities[self.ACTIVITY_ID]
                assert details["state"] == "canceled"
                assert details["canceled_id"] == canceled_id
                assert details["canceled_timestamp"] == history.events[-3].timestamp
                assert details["details"] == "given up"
                assert details["name"] == "tests.data.activities.increment"

    def test_a_canceled_workflow_execution_is_parsed(self):
        swf_history = builder.History(OneActivityWorkflow, input={})
        swf_history.add_decision_task_completed()
        swf_history.add_workflow_execution_canceled(decision_id=swf_history.last_id, details="given up")
        history = History(swf_history)

        history.parse()

        assert history.workflow["state"] == "canceled"
        assert history.workflow["canceled_id"] == swf_history.last_id
        assert history.workflow["canceled_timestamp"] == swf_history.events[-1].timestamp
        assert history.workflow["decision_task_completed_event_id"] == swf_history.last_id - 1
        assert history.workflow["details"] == "given up"


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

        assert re.search(r"^simpleflow\+s3://jumbo-bucket/[a-z0-9-]+ 90002$", result)

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
        assert re.search(r"simpleflow\+s3://jumbo-bucket/[a-z0-9-]+ \d{4,}", attrs["reason"])
        assert re.search(r"simpleflow\+s3://jumbo-bucket/[a-z0-9-]+ \d{4,}", attrs["details"])
        details = format.decode(attrs["details"])
        assert details["error"] == "ValueError"
        assert len(details["message"]) > 9 * 10_000

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
        assert isinstance(details, dict)
        assert re.search(r"^Number: 012345.*", details["message"])

        reason = format.decode(attrs["reason"], use_proxy=False)
        assert re.search(
            r"^Workflow execution error in activity-tests.test_simpleflow.swf."
            r'test_executor.print_me_n_times: "ValueError: Number: 012345679\d+"$',
            reason,
        )
