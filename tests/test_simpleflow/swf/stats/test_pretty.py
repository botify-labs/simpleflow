from __future__ import annotations

import json
import unittest
from unittest import mock

from simpleflow.history import History
from simpleflow.swf.mapper.models.history import builder
from simpleflow.swf.mapper.models.history.base import History as BasicHistory
from simpleflow.swf.stats import pretty
from simpleflow.swf.stats.pretty import dump_history_to_json
from tests.data.activities import increment
from tests.data.workflows import BaseTestWorkflow


def fake_history():
    """
    Generates a SWF's History object like the SWF decider does, but from a fake
    workflow execution history stored in a json file in tests/data/dumps/.
    """
    with open("tests/data/dumps/workflow_execution_basic.json") as f:
        basic_history_tree = json.loads(f.read())
    basic_history = BasicHistory.from_event_list(basic_history_tree["events"])
    return History(basic_history)


class TestSimpleflowSwfStatsPretty(unittest.TestCase):
    def test_dump_history_to_json(self):
        history = fake_history()
        dump = dump_history_to_json(history)
        parsed = json.loads(dump)

        self.assertEqual(
            3,
            len(parsed),
            f"we should get only one activity in the dump, got {len(parsed)}:\n{parsed}",
        )
        self.assertEqual(
            [
                "activity-examples.basic.increment-1",
                "activity-examples.basic.Delay-1",
                "activity-examples.basic.double-1",
            ],
            [t[0] for t in parsed],
        )


REFUSED_ID = "activity-tests.data.activities.double-1"
SCHEDULED_ID = "activity-tests.data.activities.increment-1"


def history_with_a_refused_schedule_and_an_unknown_cancel_request():
    """
    Activity entries without the keys of a scheduled one: no id, name nor input for
    the refused schedule, no name nor input for the cancel request.
    """
    history = builder.History(BaseTestWorkflow, input={})
    history.add_decision_task_completed()
    decision_id = history.last_id
    history.add_activity_task_schedule_failed(
        REFUSED_ID,
        decision_id,
        {"name": "tests.data.activities.double", "version": "example"},
        "ACTIVITY_TYPE_DOES_NOT_EXIST",
    )
    history.add_activity_task_cancel_requested("activity-unknown-1", decision_id)
    history.add_activity_task(increment, decision_id, last_state="scheduled", activity_id=SCHEDULED_ID)
    return history


class TestPrettyWithIncompleteActivityEntries(unittest.TestCase):
    def setUp(self):
        self.swf_history = history_with_a_refused_schedule_and_an_unknown_cancel_request()
        self.execution = mock.Mock(tag_list=[])
        self.execution.history.return_value = self.swf_history

    def test_status(self):
        _, rows = pretty.status(self.execution)

        assert [row[:2] for row in rows] == [
            ("tests.data.activities.increment", "scheduled"),
            ("activity-unknown-1", "cancel_requested"),
            ("tests.data.activities.double", "schedule_failed"),
        ]
        assert all(row[2] is not None for row in rows)

    def test_get_task(self):
        header, rows = pretty.get_task(self.execution, REFUSED_ID)

        row = dict(zip(header, rows[0]))
        assert row["id"] == REFUSED_ID
        assert row["name"] == "tests.data.activities.double"
        assert row["version"] == "example"
        assert row["state"] == "schedule_failed"
        assert row["timestamp"] is not None
        assert row["input"] is None

        header, rows = pretty.get_task(self.execution, "activity-unknown-1")

        row = dict(zip(header, rows[0]))
        assert (row["id"], row["name"], row["version"]) == ("activity-unknown-1", None, None)
        assert row["state"] == "cancel_requested"

    def test_info(self):
        header, rows = pretty.info(self.execution)

        assert dict(zip(header, rows[0]))["input"] is None
