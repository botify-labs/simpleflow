from __future__ import annotations

import hashlib
from contextvars import ContextVar
from typing import Any

import boto3
from botocore import config

from simpleflow.utils import json_dumps

_client_var: ContextVar[dict] = ContextVar("boto3_clients")


def clean_config(cfg: config.Config) -> dict[str, Any]:
    rc = {}
    for k in vars(cfg):
        if k.startswith("_"):
            continue
        v = getattr(cfg, k)
        if callable(v) or v is None:
            continue
        rc[k] = v
    return rc


def clean_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    We don't know how (or want) to serialize botocore.config.Config instances;
    they currently only contain POD.
    """
    rc = {}
    for k, v in kwargs.items():
        if isinstance(v, config.Config):
            v = clean_config(v)
        rc[k] = v
    return rc


def get_or_create_boto3_client(*, region_name: str | None, service_name: str, **kwargs: Any):
    d = {
        "region_name": region_name,
        "service_name": service_name,
    }
    cleaned_kwargs = clean_kwargs(kwargs)
    d.update(cleaned_kwargs)
    key = hashlib.sha1(json_dumps(d).encode()).hexdigest()
    boto3_clients = _client_var.get({})
    if not boto3_clients:
        _client_var.set(boto3_clients)

    client = boto3_clients.get(key)
    if client is None:
        session = boto3.session.Session(region_name=region_name)
        client = session.client(service_name, **kwargs)
        boto3_clients[key] = client
    return client
