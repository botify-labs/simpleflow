from collections import namedtuple
from mock import patch
import unittest

from moto import mock_swf

from simpleflow.swf.process.worker.base import ActivityWorker, ActivityPoller
from swf.models import Domain, ActivityTask


FakeActivityType = namedtuple("FakeActivityType", ["name"])


@mock_swf
class TestActivityWorker(unittest.TestCase):
    def test_dispatch_is_catched_correctly(self):
        domain = Domain("test-domain")
        poller = ActivityPoller(domain, "task-list")

        # this activity does not exist, so it will provoke an
        # ImportError when dispatching
        activity_type = FakeActivityType("activity.does.not.exist")
        task = ActivityTask(domain, "task-list", activity_type=activity_type)

        worker = ActivityWorker()

        with patch.object(poller, "fail_with_retry") as mock:
            worker.process(poller, "token", task)

        self.assertEquals(1, mock.call_count)
        self.assertEquals(mock.call_args[0], ("token", task))
        self.assertIn("No module named ", mock.call_args[1]["reason"])


if __name__ == '__main__':
    unittest.main()
