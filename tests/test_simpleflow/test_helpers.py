# See README for more information about integration tests
from __future__ import annotations

import unittest

from sure import expect

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
        expect(str(func)).to.match(r"^Activity\(name=tests.integration.workflow.sleep,")
        expect(args).to.equal([37])
        expect(kwargs).to.equal(
            {
                "context": {
                    "activity_id": "activity-tests.integration.workflow.sleep-1",
                    "input": {"args": [37]},
                    "name": "tests.integration.workflow.sleep",
                    "version": None,
                }
            }
        )
        expect(meta).to.equal({})
        expect(params["id"]).to.equal("activity-tests.integration.workflow.sleep-1")

    def test_find_activity_with_overriden_input(self):
        _, args, _, _, _ = find_activity(FakeHistory(), scheduled_id=5, input={"args": [4]})
        expect(args).to.equal([4])
