from sure import expect

from tests.integration import VCRIntegrationTest, vcr


class TestMarkers(VCRIntegrationTest):
    @vcr.use_cassette
    def test_without_replays(self):
        events = self.run_standalone('tests.integration.workflow.MarkerWorkflow', False)
        marker_recorded = list(filter(
            lambda e: e['eventType'] == 'MarkerRecorded',
            events
        ))
        expect(len(marker_recorded)).to.equal(3)  # 3 markers
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
        marker_recorded = list(filter(
            lambda e: e['eventType'] == 'MarkerRecorded',
            events
        ))
        expect(len((marker_recorded))).to.equal(3)  # 3 markers
        marker_details = [
            e['markerRecordedEventAttributes'].get('details') for e in marker_recorded
        ]
        expect(marker_details).to.equal([None, '"some details"', '"2nd marker\'s details"'])
        decision_task_completed_event_id = set([
            e['markerRecordedEventAttributes']['decisionTaskCompletedEventId'] for e in marker_recorded
        ])
        expect(len(decision_task_completed_event_id)).to.equal(3)  # sent in different decisions
