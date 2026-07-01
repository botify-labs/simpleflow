from __future__ import annotations

import json

from tests.integration import VCRIntegrationTest, vcr


class TestCanvas(VCRIntegrationTest):
    @vcr.use_cassette
    def test_chain_send_result(self):
        events = self.run_standalone("tests.integration.workflow.ChainTestWorkflow")
        last_event = events[-1]
        assert last_event["eventType"] == "WorkflowExecutionCompleted"
        result = json.loads(last_event["workflowExecutionCompletedEventAttributes"]["result"])
        assert result == [6, 12]

    @vcr.use_cassette
    def test_child_workflow(self):
        events = self.run_standalone("tests.integration.workflow.GroupTestWorkflowWithChild")
        last_event = events[-1]
        assert last_event["eventType"] == "WorkflowExecutionCompleted"
        result = json.loads(last_event["workflowExecutionCompletedEventAttributes"]["result"])
        assert result == [[5, 10]]
