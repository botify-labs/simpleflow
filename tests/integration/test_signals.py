from __future__ import annotations

import json

from sure import expect

from tests.integration import VCRIntegrationTest, vcr


class TestSignals(VCRIntegrationTest):
    @vcr.use_cassette
    def test_unrequested_signal(self):
        events = self.run_standalone("tests.integration.workflow.SignaledWorkflow")
        expect(events[-1]["eventType"]).should.be.equal("WorkflowExecutionCompleted")
        expect(events[-1]["workflowExecutionCompletedEventAttributes"]["result"]).should.be.equal('"signal sent!"')
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
                expect(e).should.be.equal(expected)
        expect(n).should.be.equal(1)

    @vcr.use_cassette
    def test_wait_signal(self):
        """
        Check that wait_signal fills the execution context.
        """
        events = self.run_standalone("tests.integration.workflow.WorkflowWithWaitSignal")
        expect(events[-1]["eventType"]).should.be.equal("WorkflowExecutionCompleted")  # OK end
        events = {e["eventId"]: e for e in events}  # simpler access

        # The DecisionTaskCompleted event following wait_signal should contain the right execution context
        expect(events[4]["eventType"]).should.be.equal("DecisionTaskCompleted")
        expect(events[4]["decisionTaskCompletedEventAttributes"]).should.contain("executionContext")
        execution_context = json.loads(events[4]["decisionTaskCompletedEventAttributes"]["executionContext"])
        expect(execution_context).should.be.equal({"waiting_signals": ["signal 2", "signal"]})

        expect(events[14]["eventType"]).should.be.equal("DecisionTaskCompleted")
        expect(events[14]["decisionTaskCompletedEventAttributes"]).should.contain("executionContext")
        execution_context = json.loads(events[14]["decisionTaskCompletedEventAttributes"]["executionContext"])
        expect(execution_context).should.be.equal({"waiting_signals": ["signal 2"]})

        # The DecisionTaskCompleted event following the signals should have an empty context
        expect(events[22]["eventType"]).should.be.equal("DecisionTaskCompleted")
        expect(events[22]["decisionTaskCompletedEventAttributes"]).should.contain("executionContext")
        execution_context = events[22]["decisionTaskCompletedEventAttributes"]["executionContext"]
        expect(execution_context).should.be.empty

        # The next DecisionTaskCompleted event should have no context
        expect(events[24]["eventType"]).should.be.equal("DecisionTaskCompleted")
        expect(events[24]["decisionTaskCompletedEventAttributes"]).should_not.contain("executionContext")
