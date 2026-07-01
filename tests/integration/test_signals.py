from __future__ import annotations

import json

from tests.integration import VCRIntegrationTest, vcr


class TestSignals(VCRIntegrationTest):
    @vcr.use_cassette
    def test_unrequested_signal(self):
        events = self.run_standalone("tests.integration.workflow.SignaledWorkflow")
        assert events[-1]["eventType"] == "WorkflowExecutionCompleted"
        assert events[-1]["workflowExecutionCompletedEventAttributes"]["result"] == '"signal sent!"'
        n = 0
        for e in events:
            if e["eventType"] == "WorkflowExecutionSignaled":
                n += 1
                del e["eventId"]
                del e["eventTimestamp"]
                expected = {
                    "eventType": "WorkflowExecutionSignaled",
                    "workflowExecutionSignaledEventAttributes": {
                        "externalInitiatedEventId": 0,
                        "input": "Hi there!",
                        "signalName": "unexpected",
                    },
                }
                assert e == expected
        assert n == 1

    @vcr.use_cassette
    def test_wait_signal(self):
        """
        Check that wait_signal fills the execution context.
        """
        events = self.run_standalone("tests.integration.workflow.WorkflowWithWaitSignal")
        assert events[-1]["eventType"] == "WorkflowExecutionCompleted"  # OK end
        events = {e["eventId"]: e for e in events}  # simpler access

        # The DecisionTaskCompleted event following wait_signal should contain the right execution context
        assert events[4]["eventType"] == "DecisionTaskCompleted"
        assert "executionContext" in events[4]["decisionTaskCompletedEventAttributes"]
        execution_context = json.loads(events[4]["decisionTaskCompletedEventAttributes"]["executionContext"])
        assert execution_context == {"waiting_signals": ["signal 2", "signal"]}

        assert events[14]["eventType"] == "DecisionTaskCompleted"
        assert "executionContext" in events[14]["decisionTaskCompletedEventAttributes"]
        execution_context = json.loads(events[14]["decisionTaskCompletedEventAttributes"]["executionContext"])
        assert execution_context == {"waiting_signals": ["signal 2"]}

        # The DecisionTaskCompleted event following the signals should have an empty context
        assert events[22]["eventType"] == "DecisionTaskCompleted"
        assert "executionContext" in events[22]["decisionTaskCompletedEventAttributes"]
        execution_context = events[22]["decisionTaskCompletedEventAttributes"]["executionContext"]
        assert not execution_context

        # The next DecisionTaskCompleted event should have no context
        assert events[24]["eventType"] == "DecisionTaskCompleted"
        assert "executionContext" not in events[24]["decisionTaskCompletedEventAttributes"]
