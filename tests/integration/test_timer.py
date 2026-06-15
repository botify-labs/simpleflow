from __future__ import annotations

from tests.integration import VCRIntegrationTest, vcr


class TestTimer(VCRIntegrationTest):
    @vcr.use_cassette
    def test_timer_and_cancel(self):
        events = self.run_standalone("tests.integration.workflow.TimerWorkflow")
        list_timer_started = [e for e in events if e["eventType"] == "TimerStarted"]
        assert len(list_timer_started) == 3
        assert [e["timerStartedEventAttributes"]["timerId"] for e in list_timer_started] == [
            "timer 2",
            "timer 1",
            "_simpleflow_wake_up_timer",
        ]
        list_timer_fired = [e for e in events if e["eventType"] == "TimerFired"]
        assert len(list_timer_fired) == 2
        assert list_timer_fired[0]["timerFiredEventAttributes"]["timerId"] == "timer 1"
        list_timer_canceled = [e for e in events if e["eventType"] == "TimerCanceled"]
        assert len(list_timer_canceled) == 1
        assert list_timer_canceled[0]["timerCanceledEventAttributes"]["timerId"] == "timer 2"
