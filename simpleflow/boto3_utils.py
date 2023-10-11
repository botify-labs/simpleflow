from __future__ import annotations

import hashlib
from threading import local
from typing import Any

import boto3

from simpleflow.utils import json_dumps


def get_or_create_boto3_client(*, region_name: str | None, service_name: str, **kwargs: Any):
    d = {
        "region_name": region_name,
        "service_name": service_name,
    }
    d.update(kwargs)
    key = hashlib.sha1(json_dumps(d).encode()).hexdigest()
    local_data = local()
    client = getattr(local_data, key, None)
    if client is None:
        session = boto3.session.Session(region_name=region_name)
        client = session.client(service_name, **kwargs)
        setattr(local_data, key, client)
    return client
