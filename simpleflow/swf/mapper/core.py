# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.
from __future__ import annotations

from datetime import datetime
import os
from typing import Any

import boto.swf  # noqa
import boto3
from boto.exception import NoAuthHandlerFound

# NB: import logger directly from simpleflow so we benefit from the logging
# config hosted in simpleflow. This wouldn't be the case with a standard
# "logging.getLogger(__name__)" which would write logs under the "swf" namespace
from simpleflow import logger
from simpleflow.utils import remove_none, retry

from . import settings

SETTINGS = settings.get()
RETRIES = int(os.environ.get("SWF_CONNECTION_RETRIES", "5"))


class ConnectedSWFObject:
    """Authenticated object interface"""

    __slots__ = ["region", "connection", "boto3_client"]

    region: str
    connection: boto.swf.layer1.Layer1
    boto3_client: boto3.client

    @retry.with_delay(
        nb_times=RETRIES,
        delay=retry.exponential,
        on_exceptions=(TypeError, NoAuthHandlerFound),
    )
    def __init__(self, *args, **kwargs):
        self.region = SETTINGS.get("region") or kwargs.get("region") or boto.swf.layer1.Layer1.DefaultRegionName
        # Use settings-provided keys if available, otherwise pass empty
        # dictionary to boto SWF client, which will use its default credentials
        # chain provider.
        cred_keys = ["aws_access_key_id", "aws_secret_access_key"]
        creds_ = {k: SETTINGS[k] for k in cred_keys if SETTINGS.get(k, None)}

        self.connection = kwargs.pop("connection", None) or boto.swf.connect_to_region(self.region, **creds_)
        if self.connection is None:
            raise ValueError(f"invalid region: {self.region}")

        self.boto3_client = kwargs.pop("boto3_client", None)
        if not self.boto3_client:
            session = boto3.session.Session(region_name=self.region)
            # raises EndpointConnectionError if region is wrong
            self.boto3_client = session.client("swf", **creds_)

        logger.debug(f"initiated connection to region={self.region}")

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.list_open_workflow_executions
    # written with boto3.
    def list_open_workflow_executions(
        self,
        domain: str,
        oldest_date: int,  # timestamp
        latest_date: int | None = None,  # timestamp
        tag: str | None = None,
        workflow_id: str | None = None,
        workflow_name: str | None = None,
        workflow_version: str | None = None,
        maximum_page_size: int | None = None,
        next_page_token: str | None = None,
        reverse_order: bool | None = None,
    ):
        kwargs = {
            "domain": domain,
            "startTimeFilter": {
                "oldestDate": datetime.fromtimestamp(oldest_date),
                "latestDate": datetime.fromtimestamp(latest_date) if latest_date is not None else None,
            },
            "nextPageToken": next_page_token,
            "maximumPageSize": maximum_page_size,
            "reverseOrder": reverse_order,
        }
        if workflow_name:
            kwargs["typeFilter"] = {
                "name": workflow_name,
                "version": workflow_version,
            }
        if tag:
            kwargs["tagFilter"] = {
                "name": tag,
            }
        if workflow_id:
            kwargs["executionFilter"] = {
                "workflowId": workflow_id,
            }

        return self.boto3_client.list_open_workflow_executions(
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.list_closed_workflow_executions
    # written with boto3's https://boto3.amazonaws.com/v1/documentation/api/1.28.20/reference/services/simpleflow/swf/mapper/client/list_open_workflow_executions.html
    def list_closed_workflow_executions(
        self,
        domain: str,
        start_latest_date: int | None = None,  # timestamp
        start_oldest_date: int | None = None,  # timestamp
        close_latest_date: int | None = None,  # timestamp
        close_oldest_date: int | None = None,  # timestamp
        close_status: str | None = None,
        tag: str | None = None,
        workflow_id: str | None = None,
        workflow_name: str | None = None,
        workflow_version: str | None = None,
        maximum_page_size: int | None = None,
        next_page_token: str | None = None,
        reverse_order: bool | None = None,
    ):
        kwargs = {
            "domain": domain,
            "nextPageToken": next_page_token,
            "maximumPageSize": maximum_page_size,
            "reverseOrder": reverse_order,
        }
        if start_oldest_date is not None:
            kwargs["startTimeFilter"] = {
                "oldestDate": datetime.fromtimestamp(start_oldest_date),
                "latestDate": datetime.fromtimestamp(start_latest_date) if start_latest_date is not None else None,
            }
        if close_oldest_date is not None:
            kwargs["closeTimeFilter"] = {
                "oldestDate": datetime.fromtimestamp(close_oldest_date),
                "latestDate": datetime.fromtimestamp(close_latest_date) if close_latest_date is not None else None,
            }
        if close_status:
            kwargs["closeStatusFilter"] = {
                "status": close_status,
            }
        if workflow_name:
            kwargs["typeFilter"] = {
                "name": workflow_name,
                "version": workflow_version,
            }
        if tag:
            kwargs["tagFilter"] = {
                "name": tag,
            }
        if workflow_id:
            kwargs["executionFilter"] = {
                "workflowId": workflow_id,
            }

        return self.boto3_client.list_closed_workflow_executions(**remove_none(kwargs))

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.list_workflow_types
    # written with boto3.
    def list_workflow_types(
        self,
        domain: str,
        registration_status: str,
        maximum_page_size: int | None = None,
        name: str | None = None,
        next_page_token: str | None = None,
        reverse_order: bool | None = None,
    ):
        kwargs = {
            "domain": domain,
            "registrationStatus": registration_status,
            "name": name,
            "nextPageToken": next_page_token,
            "maximumPageSize": maximum_page_size,
            "reverseOrder": reverse_order,
        }
        return self.boto3_client.list_workflow_types(**remove_none(kwargs))

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.describe_workflow_type
    # written with boto3.
    def describe_workflow_type(self, domain: str, name: str, version: str):
        return self.boto3_client.describe_workflow_type(
            domain=domain,
            workflowType={
                "name": name,
                "version": version,
            },
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.describe_workflow_execution
    # written with boto3.
    def describe_workflow_execution(self, domain: str, run_id: str, workflow_id: str):
        return self.boto3_client.describe_workflow_execution(
            domain=domain,
            execution={
                "workflowId": workflow_id,
                "runId": run_id,
            },
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.describe_domain
    # written with boto3.
    def describe_domain(self, name: str):
        return self.boto3_client.describe_domain(
            name=name,
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.describe_activity_type
    # written with boto3.
    def describe_activity_type(self, domain: str, name: str, version: str):
        return self.boto3_client.describe_activity_type(
            domain=domain,
            activityType={
                "name": name,
                "version": version,
            },
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.deprecate_activity_type
    # written with boto3.
    def deprecate_activity_type(self, domain: str, name: str, version: str):
        return self.boto3_client.deprecate_activity_type(
            domain=domain,
            activityType={
                "name": name,
                "version": version,
            },
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.deprecate_workflow_type
    # written with boto3.
    def deprecate_workflow_type(self, domain: str, name: str, version: str):
        return self.boto3_client.deprecate_workflow_type(
            domain=domain,
            workflowType={
                "name": name,
                "version": version,
            },
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.terminate_workflow_execution
    # written with boto3.
    def terminate_workflow_execution(
        self,
        domain: str,
        workflow_id: str,
        run_id: str | None = None,
        child_policy: str | None = None,
        details: str | None = None,
        reason: str | None = None,
    ):
        kwargs = {
            "runId": run_id,
            "childPolicy": child_policy,
            "details": details,
            "reason": reason,
        }
        return self.boto3_client.terminate_workflow_execution(
            domain=domain,
            workflowId=workflow_id,
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.deprecate_domain
    # written with boto3.
    def deprecate_domain(self, name: str):
        return self.boto3_client.deprecate_domain(
            name=name,
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.register_domain
    # written with boto3.
    def register_domain(
        self,
        name: str,
        workflow_execution_retention_period_in_days: str,
        description: str | None = None,
    ):
        kwargs = {
            "description": description,
        }
        return self.boto3_client.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays=workflow_execution_retention_period_in_days,
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.register_activity_type
    # written with boto3.
    def register_activity_type(
        self,
        domain: str,
        name: str,
        version: str,
        task_list: str | None = None,
        default_task_schedule_to_close_timeout: str | None = None,
        default_task_schedule_to_start_timeout: str | None = None,
        default_task_start_to_close_timeout: str | None = None,
        default_task_heartbeat_timeout: str | None = None,
        description: str | None = None,
    ):
        kwargs = {
            "defaultTaskList": {
                "name": task_list,
            },
            "defaultTaskScheduleToCloseTimeout": default_task_schedule_to_close_timeout,
            "defaultTaskScheduleToStartTimeout": default_task_schedule_to_start_timeout,
            "defaultTaskStartToCloseTimeout": default_task_start_to_close_timeout,
            "defaultTaskHeartbeatTimeout": default_task_heartbeat_timeout,
            "description": description,
        }
        return self.boto3_client.register_activity_type(
            domain=domain,
            name=name,
            version=version,
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.register_workflow_type
    # written with boto3.
    def register_workflow_type(
        self,
        domain: str,
        name: str,
        version: str,
        task_list: str | None = None,
        default_child_policy: str | None = None,
        default_execution_start_to_close_timeout: str | None = None,
        default_task_start_to_close_timeout: str | None = None,
        description: str | None = None,
    ):
        kwargs = {
            "defaultTaskList": {
                "name": task_list,
            },
            "defaultChildPolicy": default_child_policy,
            "defaultExecutionStartToCloseTimeout": default_execution_start_to_close_timeout,
            "defaultTaskStartToCloseTimeout": default_task_start_to_close_timeout,
            "description": description,
        }
        return self.boto3_client.register_workflow_type(
            domain=domain,
            name=name,
            version=version,
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.poll_for_decision_task
    # written with boto3.
    def poll_for_decision_task(
        self,
        domain: str,
        task_list: str,
        identity: str | None = None,
        maximum_page_size: int | None = None,
        next_page_token: str | None = None,
        reverse_order: bool | None = None,
        start_at_previous_started_event: bool = False,
    ):
        kwargs = {
            "identity": identity,
            "maximumPageSize": maximum_page_size,
            "nextPageToken": next_page_token,
            "reverseOrder": reverse_order,
            "startAtPreviousStartedEvent": start_at_previous_started_event,
        }
        return self.boto3_client.poll_for_decision_task(
            domain=domain,
            taskList={
                "name": task_list,
            },
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.poll_for_activity_task
    # written with boto3.
    def poll_for_activity_task(
        self,
        domain: str,
        task_list: str,
        identity: str | None = None,
    ):
        kwargs = {
            "identity": identity,
        }
        return self.boto3_client.poll_for_activity_task(
            domain=domain,
            taskList={
                "name": task_list,
            },
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.record_activity_task_heartbeat
    # written with boto3.
    def record_activity_task_heartbeat(
        self,
        task_token: str,
        details: str | None = None,
    ):
        kwargs = {
            "details": details,
        }
        return self.boto3_client.record_activity_task_heartbeat(
            taskToken=task_token,
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.respond_decision_task_completed
    # written with boto3.
    def respond_decision_task_completed(
        self,
        task_token: str,
        decisions: list[dict[str, Any]],
        execution_context: str | None = None,
    ):
        kwargs = {
            "executionContext": execution_context,
        }
        return self.boto3_client.respond_decision_task_completed(
            taskToken=task_token,
            decisions=decisions,
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.respond_activity_task_completed
    # written with boto3.
    def respond_activity_task_completed(
        self,
        task_token: str,
        result: str | None = None,
    ):
        kwargs = {
            "result": result,
        }
        return self.boto3_client.respond_activity_task_completed(
            taskToken=task_token,
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.respond_activity_task_failed
    # written with boto3.
    def respond_activity_task_failed(
        self,
        task_token: str,
        reason: str | None = None,
        details: str | None = None,
    ):
        kwargs = {
            "reason": reason,
            "details": details,
        }
        return self.boto3_client.respond_activity_task_failed(
            taskToken=task_token,
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.respond_activity_task_canceled
    # written with boto3.
    def respond_activity_task_canceled(
        self,
        task_token: str,
        details: str | None = None,
    ):
        kwargs = {
            "details": details,
        }
        return self.boto3_client.respond_activity_task_canceled(
            taskToken=task_token,
            **remove_none(kwargs),
        )

    # Proxy for https://boto.cloudhackers.com/en/latest/ref/swf.html#boto.swf.layer1.Layer1.signal_workflow_execution
    # written with boto3.
    def signal_workflow_execution(
        self,
        domain: str,
        signal_name: str,
        workflow_id: str,
        input: str | None = None,
        run_id: str | None = None,
    ):
        kwargs = {
            "input": input,
            "runId": run_id,
        }
        return self.boto3_client.signal_workflow_execution(
            domain=domain,
            signalName=signal_name,
            workflowId=workflow_id,
            **remove_none(kwargs),
        )
