from __future__ import annotations

import re

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


class TestJumboErrors(VCRIntegrationTest):
    @vcr.use_cassette
    def test_failing_activity_output(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowWithTooBigOutput")
        failures = [e for e in events if e["eventType"] == "ActivityTaskFailed"]
        expect(failures).to.have.length_of(1)
        expect(failures[0]).to.have.key("activityTaskFailedEventAttributes").should.have.key(
            "reason"
        ).being.with_value.match(r"Message too long", re.IGNORECASE)

    @vcr.use_cassette
    def test_failing_activity_input(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowWithTooBigInput")
        failures = [e for e in events if e["eventType"] == "WorkflowExecutionFailed"]
        expect(failures).to.have.length_of(1)
        expect(failures[0]).to.have.key("workflowExecutionFailedEventAttributes").should.have.key(
            "reason"
        ).being.with_value.match(r"Message too long", re.IGNORECASE)

    @vcr.use_cassette
    def test_failing_workflow_output(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowWithTooBigOutputInChild")
        failures = [e for e in events if e["eventType"] == "ChildWorkflowExecutionFailed"]
        expect(failures).to.have.length_of(1)
        expect(failures[0]).to.have.key("childWorkflowExecutionFailedEventAttributes").should.have.key(
            "reason"
        ).being.with_value.match(r"Message too long", re.IGNORECASE)

    @vcr.use_cassette
    def test_failing_workflow_input(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowWithTooBigInputInChild")
        failures = [e for e in events if e["eventType"] == "WorkflowExecutionFailed"]
        expect(failures).to.have.length_of(1)
        expect(failures[0]).to.have.key("workflowExecutionFailedEventAttributes").should.have.key(
            "reason"
        ).being.with_value.match(r"Message too long", re.IGNORECASE)
