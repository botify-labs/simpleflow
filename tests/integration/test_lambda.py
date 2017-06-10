from sure import expect

from tests.integration import VCRIntegrationTest, vcr


class TestLambda(VCRIntegrationTest):
    @vcr.use_cassette
    def test_lambda(self):
        events = self.run_standalone('tests.integration.workflow.LambdaWorkflow')
        expect(len(events)).should.equal(11)
        expect(events[0]['workflowExecutionStartedEventAttributes']).should.have.key('lambdaRole')
        expect(events[4]['eventType']).should.equal('LambdaFunctionScheduled')
        expect(events[4]['lambdaFunctionScheduledEventAttributes']['name']).should.equal('hello-world-python')
        expect(events[5]['eventType']).should.equal('LambdaFunctionStarted')
        expect(events[6]['eventType']).should.equal('LambdaFunctionCompleted')
        expect(events[6]['lambdaFunctionCompletedEventAttributes']['result']).should.equal('42')
        print(events)
