from __future__ import print_function

import time

from simpleflow import (
    activity,
    futures,
    Workflow,
)
from simpleflow.canvas import Group, Chain
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


class BaseWorkflow(Workflow):
    version = 'example'
    task_list = 'example'


# This workflow demonstrates a basic use of signals inside a single workflow.
# Not really useful, I guess, and not working with the local executor :-)
class SignalsWorkflow(BaseWorkflow):
    name = 'signals-simple'

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
class CanvasSignalsWorkflow(BaseWorkflow):
    name = 'signals-canvas'

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


class ChildWaitSignalsWorkflow(BaseWorkflow):
    """
    Wait for a signal then execute an activity.
    """
    name = 'signals-child-receiver'

    def run(self):
        all = [
            self.submit(self.wait_signal('signal1')),
            self.submit(func_a_1_1),
        ]
        futures.wait(*all)


class ChildSendSignalsWorkflow(BaseWorkflow):
    """
    Send a signal.
    """
    name = 'signals-child-sender'

    def run(self):
        all = [
            self.submit(self.signal('signal1')),
        ]
        futures.wait(*all)


class ChildWorkUntilSignalWorkflow(BaseWorkflow):
    """
    Work until receiving a signal.
    """
    name = 'signals-child-worker'
    version = 'example'
    task_list = 'example'

    def run(self):
        signal_waiter = self.submit(self.wait_signal('signal1'))
        while not signal_waiter.finished:
            fut = self.submit(func_a_1_1)
            futures.wait(fut)


# Sending a signal that a child workflow waits on.
class ParentSignalsWorkflow(Workflow):
    name = 'signals-parent'

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
class ParentSignalsWorkflow3(BaseWorkflow):
    name = 'signals-parent-3'

    def run(self):
        all = [
            self.submit(ChildSendSignalsWorkflow),
            self.submit(self.wait_signal('signal1')),
        ]
        futures.wait(*all)


# The child works until it receives a signal.
class ParentSignalsWorkflow4(BaseWorkflow):
    name = 'signals-parent-4'

    def run(self):
        all = [
            self.submit(ChildWorkUntilSignalWorkflow),
            self.submit(
                Chain(
                    ActivityTask(func_a_1_2),
                    self.signal('signal1'),
                )
            ),
        ]
        futures.wait(*all)


class ChildSignalsParentWorkflow(BaseWorkflow):
    name = 'child-workflow'

    def run(self):
        run_context = self.get_run_context()
        parent_workflow_id = run_context.get('parent_workflow_id')
        parent_run_id = run_context.get('parent_run_id')
        print(run_context)
        f = self.submit(
            self.signal(
                'ChildReady',
                workflow_id=parent_workflow_id, run_id=parent_run_id,
                me={
                    'workflow_id': run_context.get('workflow_id'),
                    'run_id': run_context.get('run_id'),
                }
            )
        )
        futures.wait(f)
        print('C1: end')


class ParentSignalsWorkflow5(BaseWorkflow):
    """
    Wait for signal emitted by the child.
    """
    name = 'signals-parent-5'

    def run(self):
        f = self.submit(ChildSignalsParentWorkflow)
        futures.wait(f)
        child_signal = self.submit(self.wait_signal('ChildReady'))
        futures.wait(child_signal)
        print('Parent: ended wait on ChildReady')
        print(child_signal.result)
        print('Parent: end')


class ChildSignalsSelfWorkflow(BaseWorkflow):
    name = 'child-workflow'

    def run(self):
        run_context = self.get_run_context()
        print(run_context)
        f1 = self.submit(
            self.signal(
                'IAmReady',
                # workflow_id=execution_context.get('workflow_id'),
                # run_id=execution_context.get('run_id'),
                propagate=False,
            )
        )
        f2 = self.submit(self.wait_signal('IAmReady'))
        futures.wait(f1, f2)
        print('C1: end')


class ParentSignalsWorkflow6(BaseWorkflow):
    """
    Assert we don't receive the child signal.
    """
    name = 'signals-parent-6'

    def run(self):
        f = self.submit(ChildSignalsSelfWorkflow)
        futures.wait(f)
        child_signal = self.submit(self.wait_signal('IAmReady'))
        assert child_signal.finished is False
        print('Parent: end')


# TODO: make integration tests of these

class ChildWorkflowSendingSignals(Workflow):
    name = 'child'
    version = 'example'
    task_list = 'example'

    def run(self):
        return self.submit(
            Chain(
                Group(
                    ChildWorkflowWaitingSignals,
                    self.signal('signal', propagate=True),
                    self.signal('signal 2', propagate=True)
                ),
            )
        ).result


class ChildWorkflowWaitingSignals(Workflow):
    name = 'child_2'
    version = 'example'
    task_list = 'example'

    def run(self):
        return self.submit(
            Chain(
                Group(
                    self.wait_signal('signal'),
                    self.wait_signal('signal 2'),
                ),
            )
        ).result


class WorkflowWithTwoChildren(Workflow):
    name = 'parent'
    version = 'example'
    task_list = 'example'

    def run(self, *args, **kwargs):
        return self.submit(
            Group(
                ChildWorkflowWaitingSignals,
                ChildWorkflowSendingSignals,
            )
        ).result
