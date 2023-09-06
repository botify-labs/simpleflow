import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_swf

from simpleflow.swf.mapper.exceptions import is_unknown


@mock_swf
def test_is_unknown():
    client = boto3.client("swf")
    client.register_domain(name="existent", workflowExecutionRetentionPeriodInDays="1")

    # Domain
    with pytest.raises(ClientError) as exception:
        client.describe_domain(name="non-existent")
    assert is_unknown("domain")(exception.value)

    # WorkflowType
    with pytest.raises(ClientError) as exception:
        client.describe_workflow_type(
            domain="existent", workflowType={"name": "non-existent", "version": "non-existent"}
        )
    assert is_unknown("WorkflowType")(exception.value)

    # ActivityType
    with pytest.raises(ClientError) as exception:
        client.describe_activity_type(
            domain="existent", activityType={"name": "non-existent", "version": "non-existent"}
        )
    assert is_unknown("ActivityType")(exception.value)

    # WorkflowExecution
    with pytest.raises(ClientError) as exception:
        client.describe_workflow_execution(
            domain="existent", execution={"workflowId": "non-existent", "runId": "non-existent"}
        )
    assert is_unknown("WorkflowExecution")(exception.value)

    # WorkflowExecution termination
    with pytest.raises(ClientError) as exception:
        client.terminate_workflow_execution(domain="existent", workflowId="non-existent")
    assert is_unknown("workflowId")(exception.value)
