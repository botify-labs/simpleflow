from simpleflow.exceptions import AggregateException
from simpleflow.utils import issubclass_
from . import futures
from .activity import Activity
from .base import Submittable, SubmittableContainer
from .task import ActivityTask, WorkflowTask

# noinspection PyUnreachableCode
if False:
    from typing import List  # NOQA


class FuncGroup(SubmittableContainer):
    """
    Class calling a function returning an ActivityTask, a group or a chain
    activities : Group, Chain...
    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = list(args)
        self.kwargs = kwargs
        self.activities = None
        self.raises_on_failure = kwargs.pop('raises_on_failure', None)

    def submit(self, executor):
        inst = self.instantiate_task()
        return executor.workflow.submit(inst)

    def instantiate_task(self):
        self.activities = self.func(*self.args, **self.kwargs)  # ivar for testing/debugging ease
        if not isinstance(self.activities, (Submittable, Group)):
            raise TypeError('FuncGroup submission should return a Group or Submittable,'
                            ' got {} instead (func: {!r})'.format(type(self.activities), self.func))

        if self.raises_on_failure is not None:
            self.activities.propagate_attribute('raises_on_failure', self.raises_on_failure)

        return self.activities

    def propagate_attribute(self, attr, val):
        setattr(self, attr, val)


class Group(SubmittableContainer):
    """
    List of activities running in parallel.
    """

    def __init__(self,
                 *activities,
                 **options):
        self.activities = []
        self.workflow_tasks = []  # type: List[WorkflowTask]
        self.max_parallel = options.pop('max_parallel', None)
        self.raises_on_failure = options.pop('raises_on_failure', None)
        self.bubbles_exception_on_failure = options.pop('bubbles_exception_on_failure', True)
        self.extend(activities)

    def append(self, submittable, *args, **kwargs):
        from simpleflow import Workflow
        if isinstance(submittable, (Submittable, SubmittableContainer)):
            if args or kwargs:
                raise ValueError('args, kwargs not supported for Submittable or SubmittableContainer')
            if isinstance(submittable, WorkflowTask):
                self.workflow_tasks.append(submittable)
        elif isinstance(submittable, Activity):
            submittable = ActivityTask(submittable, *args, **kwargs)
        elif issubclass_(submittable, Workflow):
            # We can't set the executor yet, so use None and remember it.
            submittable = WorkflowTask(None, submittable, *args, **kwargs)
            self.workflow_tasks.append(submittable)
        else:
            raise ValueError('{} should be a Submittable, Group, or Activity'.format(submittable))

        if self.raises_on_failure is not None:
            submittable.propagate_attribute('raises_on_failure', self.raises_on_failure)
        self.activities.append(submittable)

    def extend(self, iterable):
        """
        Append the specified activities.
        :param iterable: list of Submittables/Groups/tuples
        Tuples are (activity, args).
        """
        for it in iterable:
            if not isinstance(it, tuple):
                self.append(it)
            else:
                self.append(*it)

    def __iadd__(self, iterable):
        """
        += shortcut for self.extend.
        :param iterable:
        :return: self
        """
        self.extend(iterable)
        return self

    def submit(self, executor):
        self.set_workflow_tasks_executor(executor)
        return GroupFuture(self.activities, executor.workflow, self.max_parallel, self.bubbles_exception_on_failure)

    def __repr__(self):
        return '<{} at {:#x}, activities={!r}>'.format(self.__class__.__name__, id(self), self.activities)

    def propagate_attribute(self, attr, val):
        """
        Propagate attribute to all activities of the Group.
        """
        for activities in self.activities:
            activities.propagate_attribute(attr, val)

    def set_workflow_tasks_executor(self, executor):
        """
        Set the workflow tasks executor.
        :param executor:
        :return:
        """
        for wt in self.workflow_tasks:
            assert not wt.executor
            wt.executor = executor
        self.workflow_tasks = []


class GroupFuture(futures.Future):

    def __init__(self, activities, workflow, max_parallel=None, bubbles_exception_on_failure=True):
        super(GroupFuture, self).__init__()
        self.activities = activities
        self.futures = []
        self.workflow = workflow
        self.max_parallel = max_parallel
        self.bubbles_exception_on_failure = bubbles_exception_on_failure

        for a in self.activities:
            if not self.max_parallel or self._count_pending_or_running < self.max_parallel:
                future = workflow.submit(a)
                self.futures.append(future)
                if self._count_pending_or_running == self.max_parallel:
                    break

        self.sync_state()
        self.sync_result()

    def sync_state(self):
        if all(a.finished for a in self.futures) and self._futures_contain_all_activities:
            self._state = futures.FINISHED
        elif any(a.cancelled for a in self.futures):
            self._state = futures.CANCELLED
        elif any(a.running for a in self.futures):
            self._state = futures.RUNNING

    @property
    def _count_pending_or_running(self):
        return len([True for f in self.futures if f.pending or f.running])

    @property
    def _futures_contain_all_activities(self):
        return len(self.futures) == len(self.activities)

    def sync_result(self):
        self._result = []
        exceptions = []
        for future in self.futures:
            if future.finished:
                self._result.append(future.result)
                if self.bubbles_exception_on_failure is not False:
                    exception = future.exception
                    exceptions.append(exception)
            else:
                self._result.append(None)
                exceptions.append(None)
        if any(ex for ex in exceptions):
            self._exception = AggregateException(exceptions)

    @property
    def count_finished_activities(self):
        return sum(1 if a.finished else 0
                   for a in self.futures)

    def __repr__(self):
        return '<{} at {:#x}, state={state}, exception={exception}, activities={activities}, futures={futures}>'.format(
            self.__class__.__name__,
            id(self),
            state=self._state,
            exception=self._exception,
            activities=self.activities,
            futures=self.futures,
        )


class Chain(Group):
    """
    Chain a list of `ActivityTask` or callables returning Group/Chain
    Ex :
    >> chain = Chain(Task(int, 2), Task(sum, [1, 2]))
    >> chain = Chain(Task(int, 2), custom_func, Task(sum, [1, 2]))
    if `send_result` is set to True, `task.result` will be sent to
        the next task as last argument
    """
    def __init__(self,
                 *activities,
                 **options):
        self.send_result = options.pop('send_result', False)
        self.break_on_failure = options.pop('break_on_failure', True)
        if self.send_result and not self.break_on_failure:
            raise ValueError("Cannot combine send_result=True with break_on_failure=False")
        if options.get('raises_on_failure') is False and 'bubbles_exception_on_failure' not in options:
            options['bubbles_exception_on_failure'] = False  # Compatible with 10cd67f: don't break upper chains
        super(Chain, self).__init__(*activities, **options)

    def submit(self, executor):
        self.set_workflow_tasks_executor(executor)
        return ChainFuture(
            self.activities,
            executor.workflow,
            bubbles_exception_on_failure=self.bubbles_exception_on_failure,
            send_result=self.send_result,
            break_on_failure=self.break_on_failure,
        )


class ChainFuture(GroupFuture):
    # Don't call GroupFuture.__init__ on purpose
    # noinspection PyMissingConstructor
    def __init__(self, activities, workflow, bubbles_exception_on_failure, send_result, break_on_failure):
        self.activities = activities
        self.workflow = workflow
        self.bubbles_exception_on_failure = bubbles_exception_on_failure
        self._state = futures.PENDING
        self._result = None
        self._exception = None
        self.futures = []
        self._has_failed = False

        previous_result = None
        for i, a in enumerate(self.activities):
            if send_result and i > 0:
                if isinstance(a, ActivityTask):
                    # ActivityTask.args is ignored when building swf.ActivityTask (#247)
                    args = a.args + [previous_result]
                    a = ActivityTask(a.activity, *args, **a.kwargs)
                else:
                    a.args.append(previous_result)

            future = workflow.submit(a)
            self.futures.append(future)
            if not future.finished:
                break
            if future.exception and break_on_failure:
                # End this chain
                self._has_failed = True
                break
            previous_result = future.result

        self.sync_state()
        self.sync_result()

    def sync_state(self):
        if all(a.finished for a in self.futures) and (self._futures_contain_all_activities or self._has_failed):
            self._state = futures.FINISHED
        elif any(a.cancelled for a in self.futures):
            self._state = futures.CANCELLED
        elif any(a.running for a in self.futures):
            self._state = futures.RUNNING
