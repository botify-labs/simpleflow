from __future__ import annotations

import json
import unittest

from simpleflow.history import History
from simpleflow.swf.stats.pretty import dump_history_to_json
from swf.models import History as BasicHistory


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
            "we should get only one activity in the dump, got {}:\n{}".format(len(parsed), parsed),
        )
        self.assertEqual(
            [
                "activity-examples.basic.increment-1",
                "activity-examples.basic.Delay-1",
                "activity-examples.basic.double-1",
            ],
            [t[0] for t in parsed],
        )
