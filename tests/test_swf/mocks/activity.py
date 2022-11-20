from __future__ import annotations

from datetime import datetime

from swf.constants import REGISTERED
from swf.utils import datetime_timestamp, past_day


def mock_list_activity_types(*args, **kwargs):
    override_data = kwargs.pop("override_data", {})

    response = {
        "typeInfos": [
            {
                "activityType": {"name": "mocked-activity-type", "version": "0.1"},
                "creationDate": datetime_timestamp(past_day(30)),
                "deprecationDate": datetime_timestamp(datetime.now()),
                "description": "mocked-description",
                "status": REGISTERED,
            }
        ]
    }

    response.update(override_data)

    return response


def mock_describe_activity_type(*args, **kwargs):
    override_data = kwargs.pop("override_data", {})

    response = {
        "configuration": {
            "defaultTaskHeartbeatTimeout": "300",
            "defaultTaskList": {"name": "mocked-task-list"},
            "defaultTaskScheduleToCloseTimeout": "300",
            "defaultTaskScheduleToStartTimeout": "300",
            "defaultTaskStartToCloseTimeout": "300",
        },
        "typeInfo": {
            "activityType": {"name": "mocked-activity-type", "version": "0.1"},
            "creationDate": datetime_timestamp(past_day(30)),
            "deprecationDate": datetime_timestamp(datetime.now()),
            "description": "mocked-description",
            "status": REGISTERED,
        },
    }

    response.update(override_data)

    return response
