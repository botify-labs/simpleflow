from __future__ import annotations

import unittest

from swf.models import ActivityType, Domain
from swf.models.decision import ActivityTaskDecision


class TestActivityTaskDecision(unittest.TestCase):
    def setUp(self):
        self.domain = Domain("test-domain")
        self.activity_type = ActivityType(self.domain, "test-name", "test-version")

    def tearDown(self):
        pass

    def test_schedule_sets_task_priority_if_present(self):
        decision = ActivityTaskDecision()

        def attributes():
            return decision["scheduleActivityTaskDecisionAttributes"]

        decision.schedule("my-activity", self.activity_type)
        self.assertIsNone(attributes().get("taskPriority"))

        decision.schedule("my-activity", self.activity_type, task_priority="0")
        self.assertEqual(attributes().get("taskPriority"), "0")

        decision.schedule("my-activity", self.activity_type, task_priority=5)
        self.assertEqual(attributes().get("taskPriority"), "5")

        decision.schedule("my-activity", self.activity_type, task_priority=-23)
        self.assertEqual(attributes().get("taskPriority"), "-23")

        for stupid_arg in ("", "not-a-number", ["list"]):
            with self.assertRaises((ValueError, TypeError)):
                decision.schedule("my-activity", self.activity_type, task_priority=stupid_arg)
