import unittest

from flaky import flaky
from sure import expect

import simpleflow.command
from tests.integration import VCRIntegrationTest, vcr


class TestSignals(VCRIntegrationTest):
    @flaky(max_runs=2)
    @vcr.use_cassette
    def test_signal_played_once_by_default(self):
        events = self.run_standalone('tests.integration.workflow.ASignalingTestParentWorkflow', True)
        signals_initiated = filter(
            lambda e: e["eventType"] == "SignalExternalWorkflowExecutionInitiated",
            events
        )
        print(signals_initiated)
        expect(len(list(signals_initiated))).to.equal(1)

    @flaky(max_runs=2)
    @vcr.use_cassette
    def test_signal_played_twice_ignored_as_idempotent(self):
        events = self.run_standalone('tests.integration.workflow.ASignalingTestParentWorkflow', False)
        signals_initiated = filter(
            lambda e: e["eventType"] == "SignalExternalWorkflowExecutionInitiated",
            events
        )
        print(signals_initiated)
        expect(len(list(signals_initiated))).to.equal(1)


if __name__ == '__main__':
    unittest.main()
