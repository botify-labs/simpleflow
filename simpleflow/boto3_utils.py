from __future__ import annotations

import hashlib
from contextvars import ContextVar
from typing import Any

import boto3
from botocore.config import Config

from simpleflow.utils import json_dumps

_client_var: ContextVar[dict] = ContextVar("boto3_clients")

# SWF holds a long poll (PollForActivityTask, PollForDecisionTask) open for up to 60 s,
# botocore's default read timeout: a task handed out as the client gives up is lost
# until it times out. AWS asks for at least 70 s.
SWF_READ_TIMEOUT = 70
_DEFAULT_CONFIGS = {"swf": Config(read_timeout=SWF_READ_TIMEOUT)}


def get_or_create_boto3_client(*, region_name: str | None, service_name: str, **kwargs: Any):
    d = {
        "region_name": region_name,
        "service_name": service_name,
    }
    d.update(kwargs)
    key = hashlib.sha1(json_dumps(d).encode()).hexdigest()
    boto3_clients = _client_var.get({})
    if not boto3_clients:
        _client_var.set(boto3_clients)

    client = boto3_clients.get(key)
    if client is None:
        session = boto3.session.Session(region_name=region_name)
        if service_name in _DEFAULT_CONFIGS:
            kwargs.setdefault("config", _DEFAULT_CONFIGS[service_name])
        client = session.client(service_name, **kwargs)
        boto3_clients[key] = client
    return client
