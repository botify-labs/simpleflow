from sure import expect

from tests.integration import VCRIntegrationTest, vcr


class TestSignals(VCRIntegrationTest):
    @vcr.use_cassette
    def test_unrequested_signal(self):
        events = self.run_standalone('tests.integration.workflow.SignaledWorkflow')
        expect(events[-1]['eventType']).should.be.equal('WorkflowExecutionCompleted')
        expect(events[-1]['workflowExecutionCompletedEventAttributes']['result']).should.be.equal('"signal sent!"')
        n = 0
        for e in events:
            if e['eventType'] == 'WorkflowExecutionSignaled':
                n += 1
                del e['eventId']
                del e['eventTimestamp']
                expected = {
                    'eventType': 'WorkflowExecutionSignaled',
                    'workflowExecutionSignaledEventAttributes': {
                        'externalInitiatedEventId': 0,
                        'input': 'Hi there!',
                        'signalName': 'unexpected',
                    }
                }
                expect(e).should.be.equal(expected)
        expect(n).should.be.equal(1)
