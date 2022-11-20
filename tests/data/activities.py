from __future__ import annotations

from simpleflow import activity

from .constants import DEFAULT_VERSION


@activity.with_attributes(version=DEFAULT_VERSION)
def increment(x):
    return x + 1


@activity.with_attributes(version=DEFAULT_VERSION)
def double(x):
    return x * 2


@activity.with_attributes(version=DEFAULT_VERSION, idempotent=True)
def triple(x):
    return x * 3


@activity.with_attributes(version=DEFAULT_VERSION, idempotent=False)
class Tetra:
    def __init__(self, x):
        self.x = x

    def execute(self):
        return self.x * 4


@activity.with_attributes(version=DEFAULT_VERSION, retry=1)
def increment_retry(x):
    return x + 1


@activity.with_attributes(version=DEFAULT_VERSION)
def print_message(msg):
    print(f"MESSAGE: {msg}")


@activity.with_attributes(version=DEFAULT_VERSION, raises_on_failure=True)
def raise_on_failure():
    raise Exception("error")


@activity.with_attributes(version=DEFAULT_VERSION)
def raise_error():
    raise Exception("error")


@activity.with_attributes(version=DEFAULT_VERSION)
def non_pythonic(*args, **kwargs):
    pass
