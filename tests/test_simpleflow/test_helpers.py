# See README for more information about integration tests
from __future__ import annotations

import re
import unittest

from simpleflow.swf.helpers import find_activity


# some fake objects to test find_activity()
class FakeHistory:
    def __init__(self):
        self.activities = {
            "activity-tests.integration.workflow.sleep-1": {
                "id": "activity-tests.integration.workflow.sleep-1",
                "name": "tests.integration.workflow.sleep",
                "scheduled_id": 5,
                "input": {
                    "args": [37],
                },
            },
        }


class TestSimpleflowSwfHelpers(unittest.TestCase):
    def test_find_activity(self):
        func, args, kwargs, meta, params = find_activity(FakeHistory(), scheduled_id=5)
        assert re.search(r"^Activity\(name=tests.integration.workflow.sleep,", str(func))
        assert args == [37]
        assert kwargs == {
            "context": {
                "activity_id": "activity-tests.integration.workflow.sleep-1",
                "input": {"args": [37]},
                "name": "tests.integration.workflow.sleep",
                "version": None,
            }
        }
        assert meta == {}
        assert params["id"] == "activity-tests.integration.workflow.sleep-1"

    def test_find_activity_with_overriden_input(self):
        _, args, _, _, _ = find_activity(FakeHistory(), scheduled_id=5, input={"args": [4]})
        assert args == [4]

    def test_find_activity_skips_entries_without_scheduled_id_or_id(self):
        history = FakeHistory()
        history.activities = {
            "refused": {"type": "activity", "state": "schedule_failed", "cause": "ACTIVITY_TYPE_DOES_NOT_EXIST"},
            "unknown": {"type": "activity", "id": "unknown", "state": "cancel_requested"},
            **history.activities,
        }

        _, _, _, _, params = find_activity(history, scheduled_id=5)
        assert params["id"] == "activity-tests.integration.workflow.sleep-1"

        _, _, _, _, params = find_activity(history, activity_id="activity-tests.integration.workflow.sleep-1")
        assert params["scheduled_id"] == 5

        with self.assertRaisesRegex(ValueError, "Couldn't find activity"):
            find_activity(history, activity_id="refused")
        with self.assertRaisesRegex(ValueError, "Couldn't find activity"):
            find_activity(history, activity_id="unknown")
        with self.assertRaisesRegex(ValueError, "Couldn't find activity"):
            find_activity(history)
