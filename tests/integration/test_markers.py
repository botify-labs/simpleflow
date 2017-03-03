import simpleflow.command
from click.testing import CliRunner
from sure import expect

from simpleflow.utils import json_dumps
from tests.integration import VCRIntegrationTest, vcr

if False:
    from typing import List, Union
    from click.testing import Result


class TestMarkers(VCRIntegrationTest):
    def invoke(self, command, arguments):
        # type: (str, Union(str, List[str])) -> Result
        if not hasattr(self, "runner"):
            self.runner = CliRunner()
        if isinstance(arguments, str):
            arguments = arguments.split(" ")
        print('simpleflow {} {}'.format(command, ' '.join(arguments)))
        return self.runner.invoke(command, arguments, catch_exceptions=False)

    def run_standalone(self, workflow_name, *args, **kwargs):
        input = json_dumps(dict(args=args, kwargs=kwargs))
        result = self.invoke(
            simpleflow.command.cli,
            [
                "standalone",
                "--workflow-id",
                str(self.workflow_id),
                "--input",
                input,
                "--nb-deciders",
                "2",
                "--nb-workers",
                "2",
                workflow_name,
            ],
        )
        expect(result.exit_code).to.equal(0)
        lines = result.output.split("\n")
        start_line = [line for line in lines if line.startswith(self.workflow_id)][0]
        _, run_id = start_line.split(" ", 1)

        events = self.get_events(run_id)
        return events

    @vcr.use_cassette
    def test_without_replays(self):
        events = self.run_standalone('tests.integration.workflow.MarkerWorkflow', False)
        marker_recorded = filter(
            lambda e: e['eventType'] == 'MarkerRecorded',
            events
        )
        expect(len(list(marker_recorded))).to.equal(3)  # 3 markers
        marker_details = [
            e['markerRecordedEventAttributes'].get('details') for e in marker_recorded
        ]
        expect(marker_details).to.equal([None, '"some details"', '"2nd marker\'s details"'])
        decision_task_completed_event_id = set([
            e['markerRecordedEventAttributes']['decisionTaskCompletedEventId'] for e in marker_recorded
        ])
        expect(len(decision_task_completed_event_id)).to.equal(1)  # sent in 1 decision

    @vcr.use_cassette
    def test_with_replays(self):
        events = self.run_standalone('tests.integration.workflow.MarkerWorkflow', True)
        marker_recorded = filter(
            lambda e: e['eventType'] == 'MarkerRecorded',
            events
        )
        expect(len(list(marker_recorded))).to.equal(3)  # 3 markers
        marker_details = [
            e['markerRecordedEventAttributes'].get('details') for e in marker_recorded
        ]
        expect(marker_details).to.equal([None, '"some details"', '"2nd marker\'s details"'])
        decision_task_completed_event_id = set([
            e['markerRecordedEventAttributes']['decisionTaskCompletedEventId'] for e in marker_recorded
        ])
        expect(len(decision_task_completed_event_id)).to.equal(3)  # sent in different decisions
