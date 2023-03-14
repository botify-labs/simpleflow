from __future__ import annotations

from simpleflow import Workflow, activity
from simpleflow.constants import HOUR, MINUTE
from simpleflow.history import History
from simpleflow.swf.stats import WorkflowStats
from swf.models.history import builder


@activity.with_attributes(version="test")
def increment(x):
    return x + 1


class ATestWorkflow(Workflow):
    name = "test_workflow"
    version = "test_version"
    task_list = "test_task_list"
    decision_tasks_timeout = 5 * MINUTE
    execution_timeout = 1 * HOUR

    def run(self, **context):
        return 0


def test_last_state_times():
    """
    This test checks an execution with a single activity tasks.

    """
    history_builder = builder.History(ATestWorkflow)

    last_state = "completed"
    activity_id = "activity-tests.test_simpleflow.test_dataflow.increment-1"

    history_builder.add_activity_task(
        increment,
        decision_id=0,
        last_state=last_state,
        activity_id=activity_id,
    )

    history = History(history_builder)
    stats = WorkflowStats(history)
    total_time = stats.total_time()

    events = history.events
    assert total_time == (events[-1].timestamp - events[0].timestamp).total_seconds()
    timings = stats.get_timings()[0]
    assert timings[0] == activity_id
    assert timings[1] == last_state

    TIMING_SCHEDULED = 2
    TIMING_STARTED = 3
    TIMING_COMPLETED = 4

    EV_SCHEDULED = -3
    EV_STARTED = -2
    EV_COMPLETED = -1

    assert timings[TIMING_SCHEDULED] == events[EV_SCHEDULED].timestamp
    assert timings[TIMING_STARTED] == events[EV_STARTED].timestamp
    assert timings[TIMING_COMPLETED] == events[EV_COMPLETED].timestamp

    TIMING_DURATION = 5
    assert timings[TIMING_DURATION] == (events[EV_COMPLETED].timestamp - events[EV_STARTED].timestamp).total_seconds()

    timings = stats.get_timings_with_percentage()[0]
    TIMING_TOTAL_TIME = -2
    TIMING_PERCENTAGE = -1
    percentage = (timings[TIMING_TOTAL_TIME] / total_time) * 100.0
    assert percentage == timings[TIMING_PERCENTAGE]
