from __future__ import print_function

import time

from simpleflow import (
    activity,
    futures,
    Workflow,
)
from simpleflow.canvas import Group, Chain
# from simpleflow.signal import Signal
from simpleflow.task import ActivityTask


@activity.with_attributes(task_list='quickstart', version='example')
def func_a_1_1(*args, **kwargs):
    print('func_a_1_1({}, {})'.format(args, kwargs))
    time.sleep(1)
    print('end func_a_1_1')
    return 'func_a_1_1'


@activity.with_attributes(task_list='quickstart', version='example')
def func_a_1_2(*args, **kwargs):
    print('func_a_1_2({}, {})'.format(args, kwargs))
    time.sleep(10)
    print('end func_a_1_2')
    return 'func_a_1_2'


@activity.with_attributes(task_list='quickstart', version='example')
def func_a_2_1(*args, **kwargs):
    print('func_a_2_1({}, {})'.format(args, kwargs))
    time.sleep(1)
    print('end func_a_2_1')
    return 'func_a_2_1'


@activity.with_attributes(task_list='quickstart', version='example')
def func_a_2_2(*args, **kwargs):
    print('func_a_2_2({}, {})'.format(args, kwargs))
    time.sleep(1)
    print('end func_a_2_2')
    return 'func_a_2_2'


@activity.with_attributes(task_list='quickstart', version='example')
def func_b_1_1(*args, **kwargs):
    print('func_b_1_1({}, {})'.format(args, kwargs))
    time.sleep(1)
    print('end func_b_1_1')
    return 'func_b_1_1'


@activity.with_attributes(task_list='quickstart', version='example')
def func_b_1_2(*args, **kwargs):
    print('func_b_1_2({}, {})'.format(args, kwargs))
    time.sleep(1)
    print('end func_b_1_2')
    return 'func_b_1_2'


@activity.with_attributes(task_list='quickstart', version='example')
def func_b_2_1(*args, **kwargs):
    print('func_b_2_1({}, {})'.format(args, kwargs))
    time.sleep(1)
    print('end func_b_2_1')
    return 'func_b_2_1'


@activity.with_attributes(task_list='quickstart', version='example')
def func_b_2_2(*args, **kwargs):
    print('func_b_2_2({}, {})'.format(args, kwargs))
    time.sleep(1)
    print('end func_b_2_2')
    return 'func_b_2_2'


# This workflow demonstrates a basic use of signals inside a single workflow.
# Not really useful, I guess :-)
class SignalsWorkflow(Workflow):
    name = 'signals-simple'
    version = 'example'
    task_list = 'example'

    def run(self):
        all = [
            self.submit(self.wait_signal('signal1')),
            self.submit(func_a_1_2),
            self.submit(self.signal('signal1')),
            # self.submit(Signal('signal1')),  # The other way; FIXME remove?
        ]
        futures.wait(*all)


# This workflow demonstrates the use of simpleflow's Chains, Groups and signals.
#
# A `Group` wraps a list of tasks that can be executed in parallel. It
# returns a future that is considered "finished" only once ALL the tasks
# in the group are finished.
#
# A `Chain` wraps a list of tasks that need to be executed sequentially.
# As groups, it returns a future that is considered "finished" only
# when all the tasks inside the Chain are finished.
class CanvasSignalsWorkflow(Workflow):
    name = 'signals-canvas'
    version = 'example'
    task_list = 'example'

    def run(self):
        chain1 = Chain(
            Group(ActivityTask(func_a_1_1), ActivityTask(func_a_1_2)),
            self.signal("signal1"),
            Group(ActivityTask(func_a_2_1), ActivityTask(func_a_2_2)),
        )
        chain2 = Chain(
            Group(ActivityTask(func_b_1_1), ActivityTask(func_b_1_2)),
            self.wait_signal("signal1"),
            Group(ActivityTask(func_b_2_1), ActivityTask(func_b_2_2)),
        )
        my_group = Group(chain1, chain2)
        fut = self.submit(my_group)
        futures.wait(fut)


class ChildWaitSignalsWorkflow(Workflow):
    name = 'signals-child-receiver'
    version = 'example'
    task_list = 'example'

    def run(self):
        all = [
            self.submit(self.wait_signal('signal1')),
            self.submit(func_a_1_1),
        ]
        futures.wait(*all)


class ChildSendSignalsWorkflow(Workflow):
    name = 'signals-child-sender'
    version = 'example'
    task_list = 'example'

    def run(self):
        all = [
            self.submit(self.signal('signal1')),
            self.submit(self.wait_signal('signal1')),
        ]
        futures.wait(*all)


# Sending a signal that a child workflow waits on.
class ParentSignalsWorkflow(Workflow):
    name = 'signals-parent'
    version = 'example'
    task_list = 'example'

    def run(self):
        all = [
            self.submit(ChildWaitSignalsWorkflow),
            self.submit(self.signal('signal1')),
        ]
        futures.wait(*all)


# One child sends a signal, the other one waits on it.
class ParentSignalsWorkflow2(Workflow):
    name = 'signals-parent-2'
    version = 'example'
    task_list = 'example'

    def run(self):
        all = [
            self.submit(ChildWaitSignalsWorkflow),
            self.submit(ChildSendSignalsWorkflow),
        ]
        futures.wait(*all)


# The child sends a signal, the parent waits on it.
class ParentSignalsWorkflow3(Workflow):
    name = 'signals-parent-3'
    version = 'example'
    task_list = 'example'

    def run(self):
        all = [
            self.submit(ChildSendSignalsWorkflow),
            self.submit(self.wait_signal('signal1')),
        ]
        futures.wait(*all)
