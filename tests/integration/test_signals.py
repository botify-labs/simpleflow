import unittest

from click.testing import CliRunner
from flaky import flaky
from sure import expect

import simpleflow.command
from tests.integration import VCRIntegrationTest, vcr


class TestSignals(VCRIntegrationTest):
    def invoke(self, command, arguments):
        if not hasattr(self, "runner"):
            self.runner = CliRunner()
        return self.runner.invoke(command, arguments.split(" "))

    @flaky(max_runs=2)
    @vcr.use_cassette
    def test_signal_played_once_by_default(self):
        result = self.invoke(
            simpleflow.command.cli,
            'standalone --workflow-id %s'
            ' --input {"args":[true]}'
            ' --nb-deciders 1 --nb-workers 1'
            ' tests.integration.workflow.ASignalingTestParentWorkflow' % (
                self.workflow_id,
            )
        )
        expect(result.exit_code).to.equal(0)
        lines = result.output.split("\n")
        start_line = [line for line in lines if line.startswith(self.workflow_id)][0]
        _, run_id = start_line.split(" ", 1)

        events = self.get_events(run_id)
        signals_initiated = filter(
            lambda e: e["eventType"] == "SignalExternalWorkflowExecutionInitiated",
            events
        )
        expect(len(list(signals_initiated))).to.equal(1)

    @flaky(max_runs=2)
    @vcr.use_cassette
    def test_signal_played_twice(self):
        result = self.invoke(
            simpleflow.command.cli,
            'standalone --workflow-id %s'
            ' --input {"args":[false]}'
            ' --nb-deciders 1 --nb-workers 1'
            ' tests.integration.workflow.ASignalingTestParentWorkflow' % (
                self.workflow_id,
            )
        )
        expect(result.exit_code).to.equal(0)
        lines = result.output.split("\n")
        start_line = [line for line in lines if line.startswith(self.workflow_id)][0]
        _, run_id = start_line.split(" ", 1)

        events = self.get_events(run_id)
        signals_initiated = filter(
            lambda e: e["eventType"] == "SignalExternalWorkflowExecutionInitiated",
            events
        )
        expect(len(list(signals_initiated))).to.equal(2)


if __name__ == '__main__':
    unittest.main()
