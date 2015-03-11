#!/usr/bin/env python
# -*- coding: utf-8 -*-


from simpleflow import futures
from simpleflow.swf.futures import Future


def test_future_init_state():
    assert Future()._state == futures.PENDING


def test_future_init_result():
    assert Future()._result is None


def test_future_init_repr():
    future = Future()
    assert repr(future) == '<Future at {} state=pending>'.format(
        hex(id(future)))


def test_future_init_cancelled():
    assert Future().cancelled() is False


def test_future_init_running():
    assert Future().running() is False


def test_future_init_done():
    assert Future().done() is False


def test_future_cancel():
    future = Future()
    assert future.cancel()
    assert future._state == futures.CANCELLED
    assert future.running() is False
    assert future.cancelled()
    assert future.done()
