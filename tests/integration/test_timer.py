from __future__ import annotations

from sure import expect

from tests.integration import VCRIntegrationTest, vcr


class TestTimer(VCRIntegrationTest):
    @vcr.use_cassette
    def test_timer_and_cancel(self):
        events = self.run_standalone("tests.integration.workflow.TimerWorkflow")
        list_timer_started = [e for e in events if e["eventType"] == "TimerStarted"]
        expect(len(list_timer_started)).to.equal(3)
        expect([e["timerStartedEventAttributes"]["timerId"] for e in list_timer_started]).to.equal(
            ["timer 2", "timer 1", "_simpleflow_wake_up_timer"]
        )
        list_timer_fired = [e for e in events if e["eventType"] == "TimerFired"]
        expect(len(list_timer_fired)).to.equal(2)
        expect(list_timer_fired[0]["timerFiredEventAttributes"]["timerId"]).to.equal("timer 1")
        list_timer_canceled = [e for e in events if e["eventType"] == "TimerCanceled"]
        expect(len(list_timer_canceled)).to.equal(1)
        expect(list_timer_canceled[0]["timerCanceledEventAttributes"]["timerId"]).to.equal("timer 2")
