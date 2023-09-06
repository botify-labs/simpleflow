from __future__ import annotations

import unittest
from collections import namedtuple
from unittest.mock import patch

from moto import mock_swf

from simpleflow.swf.process.worker.base import ActivityPoller, ActivityWorker
from simpleflow.swf.mapper.models.activity import ActivityTask
from simpleflow.swf.mapper.models.domain import Domain

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

        self.assertEqual(1, mock.call_count)
        self.assertEqual(mock.call_args[0], ("token", task))
        self.assertIn("unable to import ", mock.call_args[1]["reason"])


if __name__ == "__main__":
    unittest.main()
