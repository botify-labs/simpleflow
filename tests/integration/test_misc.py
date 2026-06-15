from __future__ import annotations

import re

from tests.integration import VCRIntegrationTest, vcr


class TestMisc(VCRIntegrationTest):
    @vcr.use_cassette
    def test_cancel_requested(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowToCancel", True)
        cancel_requested = [e for e in events if e["eventType"] == "WorkflowExecutionCancelRequested"]
        assert len(cancel_requested) == 1
        assert events[-1]["eventType"] == "WorkflowExecutionCanceled"

    @vcr.use_cassette
    def test_cancel_requested_refused(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowToCancel", False)
        cancel_requested = [e for e in events if e["eventType"] == "WorkflowExecutionCancelRequested"]
        assert len(cancel_requested) == 1
        assert events[-1]["eventType"] == "WorkflowExecutionCompleted"


class TestJumboErrors(VCRIntegrationTest):
    @vcr.use_cassette
    def test_failing_activity_output(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowWithTooBigOutput")
        failures = [e for e in events if e["eventType"] == "ActivityTaskFailed"]
        assert len(failures) == 1
        assert re.search(r"Message too long", failures[0]["activityTaskFailedEventAttributes"]["reason"], re.IGNORECASE)

    @vcr.use_cassette
    def test_failing_activity_input(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowWithTooBigInput")
        failures = [e for e in events if e["eventType"] == "WorkflowExecutionFailed"]
        assert len(failures) == 1
        assert re.search(
            r"Message too long", failures[0]["workflowExecutionFailedEventAttributes"]["reason"], re.IGNORECASE
        )

    @vcr.use_cassette
    def test_failing_workflow_output(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowWithTooBigOutputInChild")
        failures = [e for e in events if e["eventType"] == "ChildWorkflowExecutionFailed"]
        assert len(failures) == 1
        assert re.search(
            r"Message too long", failures[0]["childWorkflowExecutionFailedEventAttributes"]["reason"], re.IGNORECASE
        )

    @vcr.use_cassette
    def test_failing_workflow_input(self):
        events = self.run_standalone("tests.integration.workflow.WorkflowWithTooBigInputInChild")
        failures = [e for e in events if e["eventType"] == "WorkflowExecutionFailed"]
        assert len(failures) == 1
        assert re.search(
            r"Message too long", failures[0]["workflowExecutionFailedEventAttributes"]["reason"], re.IGNORECASE
        )
