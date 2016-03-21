#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import swf.models

from simpleflow import (
    activity,
    Workflow,
    futures,
)

from .data import (
    DOMAIN,
    DEFAULT_VERSION,
)


@activity.with_attributes(version=DEFAULT_VERSION)
def increment(x):
    return x + 1


@activity.with_attributes(version=DEFAULT_VERSION)
def double(x):
    return x * 2


class TestWorkflow(Workflow):
    name = 'test_workflow'
    version = 'test_version'
    task_list = 'test_task_list'
    decision_tasks_timeout = '300'
    execution_timeout = '3600'
    tag_list = None      # FIXME should be optional
    child_policy = None  # FIXME should be optional


class TestDefinition(TestWorkflow):
    """
    Executes two tasks. The second depends on the first.

    """
    def run(self):
        a = self.submit(increment, 1)
        assert isinstance(a, futures.Future)

        b = self.submit(double, a)

        return b.result
