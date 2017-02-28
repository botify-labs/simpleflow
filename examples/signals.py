from __future__ import print_function

import time

import abc

from simpleflow import (
    activity,
    futures,
    Workflow,
)
from simpleflow import swf
from simpleflow.canvas import Group, Chain
from simpleflow.task import ActivityTask
from simpleflow.utils import json_dumps

SIGNAL_1 = 'signal1'


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

    def __init__(self, executor):
        super(BaseWorkflow, self).__init__(executor)
        self.futures = []

    def submit_add(self, func, *args, **kwargs):
        f = self.submit(func, *args, **kwargs)
        self.futures.append(f)

    def submit_and_wait(self, func, *args, **kwargs):
        f = self.submit(func, *args, **kwargs)
        return f.result

    def wait_all(self):
        futures.wait(*self.futures)

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        pass


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
        execution_context = self.get_execution_context()
        parent_workflow_id = execution_context.get('parent_workflow_id')
        parent_run_id = execution_context.get('parent_run_id')
        print(execution_context)
        f = self.submit(
            self.signal(
                'ChildReady',
                workflow_id=parent_workflow_id, run_id=parent_run_id,
                me={
                    'workflow_id': execution_context.get('workflow_id'),
                    'run_id': execution_context.get('run_id'),
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
        execution_context = self.get_execution_context()
        print(execution_context)
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


class ChildSignalsSelfWorkflow2(BaseWorkflow):
    name = 'child-workflow'

    def run(self):
        execution_context = self.get_execution_context()
        parent_workflow_id = execution_context['parent_workflow_id']
        parent_run_id = execution_context['parent_run_id']
        # FIXME BUG: this signal sent at 10 and 16?
        self.submit(
            self.signal(
                SIGNAL_1,
                # workflow_id=parent_workflow_id,
                # run_id=parent_run_id,
                propagate=True,
                args=["1"]
            )
        )
        self.submit(self.wait_signal(SIGNAL_1))
        self.submit_and_wait(func_a_1_1, "1")
        self.signal_immediate(SIGNAL_1, parent_workflow_id, parent_run_id, "2")
        self.signal_immediate(SIGNAL_1, parent_workflow_id, parent_run_id, "3")
        self.submit_and_wait(func_a_1_1, "2")

    def signal_immediate(self, signal_name, workflow_id, run_id, *args, **kwargs):
        self.executor._execution.signal(
            signal_name=signal_name,
            input={
                "input": {
                    "args": args,
                    "kwargs": kwargs
                }
            },
            workflow_id=workflow_id,
            run_id=run_id,
        )


class ParentSignalsWorkflow7(BaseWorkflow):
    """
    Test: re-process signal if new.
    """
    name = 'signals-parent'

    def __init__(self, executor):
        super(ParentSignalsWorkflow7, self).__init__(executor)
        self.futures = []

    def run(self):
        ex = self.executor
        if not isinstance(ex, swf.executor.Executor):
            raise Exception('only works on SWF')
        self.submit_add(ChildSignalsSelfWorkflow2)
        self.submit_and_wait(func_a_1_1)

        self.check_signal(1)
        self.submit_add(func_a_1_2)
        self.wait_all()
        self.check_signal(2)
        self.wait_all()
        self.check_signal(3)

    def check_signal(self, i):
        ss = self.get_signal_instances_since_last_replay(SIGNAL_1)
        if ss:
            print('Just got {} ({}):\n{}'.format(SIGNAL_1, i, '\n'.join(str(s) for s in ss)))

    def get_signal_instances_since_last_replay(self, signal_name):
        ex = self.executor
        assert isinstance(ex, swf.executor.Executor)
        signals = ex._history.signals
        if not signals:
            print('no signal')
            return []
        sigs = signals.get(signal_name)
        if not sigs:
            print('{} not received'.format(signal_name))
            return []
        previous_started_event_id = ex._previous_started_event_id or 0
        sig = sigs[-1]
        print('signal {} last received at {}; _started_event_id={}; _previous_started_event_id={}; rc={}'.format(
            signal_name,
            sig['event_id'],
            ex._started_event_id,
            previous_started_event_id,
            sig['event_id'] > previous_started_event_id,
        ))
        sigs = [sig for sig in sigs if sig['event_id'] > previous_started_event_id]
        return sigs

        # def just_got_signal(self, signal_name):
        #     ex = self.executor
        #     assert isinstance(ex, swf.executor.Executor)
        #     signals = ex._history.signals
        #     if not signals:
        #         print('no signal')
        #         return
        #     sigs = signals.get(signal_name)
        #     if not sigs:
        #         print('{} not received'.format(signal_name))
        #         return
        #     previous_started_event_id = ex._previous_started_event_id
        #     sig = sigs[-1]
        #     print('signal {} last received at {}; _started_event_id={}; _previous_started_event_id={}; rc={}'.format(
        #         signal_name,
        #         sig['event_id'],
        #         ex._started_event_id,
        #         previous_started_event_id,
        #         previous_started_event_id is None or sig['event_id'] > previous_started_event_id,
        #     ))
        #     return previous_started_event_id is None or sig['event_id'] > previous_started_event_id
