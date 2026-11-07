from __future__ import annotations

import pytest

from simpleflow import boto3_utils
from simpleflow.boto3_utils import SWF_READ_TIMEOUT, get_or_create_boto3_client


@pytest.fixture(autouse=True)
def fresh_client_cache():
    token = boto3_utils._client_var.set({})
    yield
    boto3_utils._client_var.reset(token)


def test_swf_client_outlasts_the_long_poll():
    client = get_or_create_boto3_client(region_name="us-east-1", service_name="swf")

    assert client.meta.config.read_timeout == SWF_READ_TIMEOUT
    assert SWF_READ_TIMEOUT > 60


def test_other_services_keep_botocore_defaults():
    client = get_or_create_boto3_client(region_name="us-east-1", service_name="s3")

    assert client.meta.config.read_timeout == 60
