# See README for more informations about integration tests
import unittest

from sure import expect

from simpleflow.swf.helpers import find_activity


# some fake objects to test find_activity()
class FakeHistory(object):
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
        func, args, kwargs, params = find_activity(FakeHistory(), scheduled_id=5)
        expect(func.__name__).to.equal("sleep")
        expect(args).to.equal([37])
        expect(kwargs).to.equal({})
        expect(params["id"]).to.equal("activity-tests.integration.workflow.sleep-1")

    def test_find_activity_with_overriden_input(self):
        _, args, _, _ = find_activity(FakeHistory(), scheduled_id=5, input={"args": [4]})
        expect(args).to.equal([4])
