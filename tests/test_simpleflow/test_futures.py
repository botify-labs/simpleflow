from __future__ import annotations

from simpleflow import futures
from simpleflow.futures import Future


def test_future_init_state():
    assert Future()._state == futures.PENDING


def test_future_init_result():
    assert Future()._result is None


def test_future_init_repr():
    future = Future()
    assert repr(future) == f"<Future at {hex(id(future))} state=pending>"


def test_future_init_cancelled():
    assert Future().cancelled is False


def test_future_init_running():
    assert Future().running is False


def test_future_init_done():
    assert Future().done is False


def test_future_cancel():
    future = Future()
    assert future.cancel()
    assert future._state == futures.CANCELLED
    assert future.running is False
    assert future.cancelled
    assert future.done
