from __future__ import annotations

import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from simpleflow.swf.mapper.core import ConnectedSWFObject
from simpleflow.swf.mapper.models.activity import ActivityType
from simpleflow.swf.mapper.models.domain import Domain

from ..mocks.activity import mock_describe_activity_type


def throw(exception):
    raise exception


class TestActivityType(unittest.TestCase):
    def setUp(self):
        self.domain = Domain("test-domain")
        self.activity_type = ActivityType(self.domain, "test-name", "test-version")

    def tearDown(self):
        pass

    def test_activity_type__diff_with_different_activity_type(self):
        with patch.object(
            ConnectedSWFObject,
            "describe_activity_type",
            mock_describe_activity_type,
        ):
            activity = ActivityType(self.domain, "different-activity", version="different-version")
            diffs = activity._diff()

            self.assertIsNotNone(diffs)
            self.assertEqual(len(diffs), 10)

            self.assertTrue(hasattr(diffs[0], "attr"))
            self.assertTrue(hasattr(diffs[0], "local"))
            self.assertTrue(hasattr(diffs[0], "upstream"))

    def test_activity_type__diff_with_identical_activity_type(self):
        with patch.object(
            ConnectedSWFObject,
            "describe_activity_type",
            mock_describe_activity_type,
        ):
            mocked = mock_describe_activity_type()
            activity = ActivityType(
                self.domain,
                name=mocked["typeInfo"]["activityType"]["name"],
                version=mocked["typeInfo"]["activityType"]["version"],
                status=mocked["typeInfo"]["status"],
                description=mocked["typeInfo"]["description"],
                creation_date=mocked["typeInfo"]["creationDate"],
                deprecation_date=mocked["typeInfo"]["deprecationDate"],
                task_list=mocked["configuration"]["defaultTaskList"]["name"],
                task_heartbeat_timeout=mocked["configuration"]["defaultTaskHeartbeatTimeout"],
                task_schedule_to_close_timeout=mocked["configuration"]["defaultTaskScheduleToCloseTimeout"],
                task_schedule_to_start_timeout=mocked["configuration"]["defaultTaskScheduleToStartTimeout"],
                task_start_to_close_timeout=mocked["configuration"]["defaultTaskStartToCloseTimeout"],
            )

            # We remove dates because they sometimes vary by 1 second due to usage of datetime.*now() in mock
            diffs = activity._diff(ignore_fields=["creation_date", "deprecation_date"])

            self.assertEqual(len(diffs), 0)

    def test_exists_with_existing_activity_type(self):
        with patch.object(ConnectedSWFObject, "describe_activity_type"):
            self.assertTrue(self.activity_type.exists)

    def test_exists_with_non_existent_activity_type(self):
        with patch.object(self.activity_type, "describe_activity_type") as mock:
            mock.side_effect = lambda *_, **__: throw(
                ClientError(
                    {
                        "Error": {
                            "Message": "Unknown type: ActivityType=[name=blah, version=test]",
                            "Code": "UnknownResourceFault",
                        },
                        "message": "Unknown type: ActivityType=[name=blah, version=test]",
                    },
                    "describe_activity_type",
                )
            )
            self.assertFalse(self.activity_type.exists)

    def test_is_synced_over_non_existent_activity_type(self):
        with patch.object(
            ConnectedSWFObject,
            "describe_activity_type",
            mock_describe_activity_type,
        ):
            domain = ActivityType(self.domain, "non-existent-activity", version="non-existent-version")
            self.assertFalse(domain.is_synced)

    def test_changes_with_different_activity_type(self):
        with patch.object(
            ConnectedSWFObject,
            "describe_activity_type",
            mock_describe_activity_type,
        ):
            activity_type = ActivityType(
                self.domain,
                "different-activity-type",
                version="different-activity-type-version",
            )
            diffs = activity_type.changes

            self.assertIsNotNone(diffs)
            self.assertEqual(len(diffs), 10)

            self.assertTrue(hasattr(diffs[0], "attr"))
            self.assertTrue(hasattr(diffs[0], "local"))
            self.assertTrue(hasattr(diffs[0], "upstream"))
