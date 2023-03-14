from __future__ import annotations

from sure import expect

from tests.integration import VCRIntegrationTest, vcr


class TestMisc(VCRIntegrationTest):
    @vcr.use_cassette
    def test_cancel_requested(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowToCancel", True)
        cancel_requested = [e for e in events if e["eventType"] == "WorkflowExecutionCancelRequested"]
        expect(len(cancel_requested)).to.equal(1)
        expect(events[-1]["eventType"]).to.equal("WorkflowExecutionCanceled")

    @vcr.use_cassette
    def test_cancel_requested_refused(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowToCancel", False)
        cancel_requested = [e for e in events if e["eventType"] == "WorkflowExecutionCancelRequested"]
        expect(len(cancel_requested)).to.equal(1)
        expect(events[-1]["eventType"]).to.equal("WorkflowExecutionCompleted")
