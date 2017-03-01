from . import futures
from .activity import Activity
from .base import Submittable, SubmittableContainer
from .signal import WaitForSignal
from .task import ActivityTask, SignalTask


def propagate_attribute(obj, attr, val):
    if isinstance(obj, Activity):
        setattr(obj, attr, val)
    elif isinstance(obj, ActivityTask):
        setattr(obj.activity, attr, val)
    elif isinstance(obj, Group):
        for activities in obj.activities:
            propagate_attribute(activities, attr, val)
    elif isinstance(obj, FuncGroup):
        setattr(obj, attr, val)
    elif isinstance(obj, (SignalTask, WaitForSignal)):
        pass
    else:
        raise Exception('Cannot propagate attribute for unknown type: {}'.format(type(obj)))


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
        self.activities = self.func(*self.args, **self.kwargs)
        if self.raises_on_failure is not None:
            propagate_attribute(self.activities, 'raises_on_failure', self.raises_on_failure)
        if not isinstance(self.activities, (Submittable, Group)):
            raise TypeError('FuncGroup submission should return a Group or Submittable,'
                            ' got {} instead'.format(type(self.activities)))
        return self.activities


class AggregateException(Exception):
    """
    Class containing a list of exceptions.

    :type exceptions: list[Exception]
    """
    def __init__(self, exceptions):
        self.exceptions = exceptions

    def append(self, ex):
        self.exceptions.append(ex)

    def handle(self, handler, *args, **kwargs):
        """
        Invoke a user-defined handler on each exception.
        :param handler: Predicate accepting an exception and returning True if it's been handled.
        :type handler: (Exception) -> bool
        :param args: args for the handler
        :param kwargs: kwargs for the handler
        :raise: new AggregateException with the unhandled exceptions, if any
        """
        unhandled_exceptions = []
        for ex in self.exceptions:
            if ex and not handler(ex, *args, **kwargs):
                unhandled_exceptions.append(ex)
        if unhandled_exceptions:
            raise AggregateException(unhandled_exceptions)

    def flatten(self):
        """
        Flatten the AggregateException. Return a new instance without inner AggregateException.
        :return:
        :rtype: AggregateException
        """
        flattened_exceptions = []
        self._flatten(self, flattened_exceptions)
        return AggregateException(flattened_exceptions)

    @staticmethod
    def _flatten(exception, exceptions):
        if isinstance(exception, AggregateException):
            for ex in exception.exceptions:
                if ex:
                    AggregateException._flatten(ex, exceptions)
        else:
            exceptions.append(exception)

    def __repr__(self):
        return repr([repr(ex) for ex in self.exceptions])

    def __str__(self):
        return str([str(ex) for ex in self.exceptions])

    def __eq__(self, other):
        return self.exceptions == other.exceptions


class Group(SubmittableContainer):
    """
    List of activities running in parallel.
    """

    def __init__(self,
                 *activities,
                 **options):
        self.activities = []
        self.max_parallel = options.pop('max_parallel', None)
        self.raises_on_failure = options.pop('raises_on_failure', None)
        self.extend(activities)

    def append(self, submittable, *args, **kwargs):
        if isinstance(submittable, (Submittable, SubmittableContainer)):
            if args or kwargs:
                raise ValueError('args, kwargs not supported for Submittable or SubmittableContainer')
            if self.raises_on_failure is not None:
                propagate_attribute(submittable, 'raises_on_failure', self.raises_on_failure)
            self.activities.append(submittable)
        elif isinstance(submittable, Activity):
            if self.raises_on_failure is not None:
                propagate_attribute(submittable, 'raises_on_failure', self.raises_on_failure)
            self.activities.append(ActivityTask(submittable, *args, **kwargs))
        else:
            raise ValueError('{} should be a Submittable, Group, or Activity'.format(submittable))

    def extend(self, iterable):
        """
        Append the specified activities.
        :param iterable: list of Submittables/Groups/tuples
        Tuples are (activity, [args, [kwargs]]).
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
        return GroupFuture(self.activities, executor.workflow, self.max_parallel)


class GroupFuture(futures.Future):

    def __init__(self, activities, workflow, max_parallel=None, raises_on_failure=True):
        super(GroupFuture, self).__init__()
        self.activities = activities
        self.futures = []
        self.workflow = workflow
        self.max_parallel = max_parallel
        self.raises_on_failure = raises_on_failure

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
                if self.raises_on_failure is not False:
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
        super(Chain, self).__init__(*activities, **options)

    def submit(self, executor):
        return ChainFuture(
            self.activities,
            executor.workflow,
            raises_on_failure=self.raises_on_failure,
            send_result=self.send_result,
        )


class ChainFuture(GroupFuture):
    def __init__(self, activities, workflow, raises_on_failure=True, send_result=False):
        self.activities = activities
        self.workflow = workflow
        self.raises_on_failure = raises_on_failure
        self._state = futures.PENDING
        self._result = None
        self._exception = None
        self.futures = []
        self._has_failed = False

        previous_result = None
        for i, a in enumerate(self.activities):
            if send_result and i > 0:
                a.args.append(previous_result)
            future = workflow.submit(a)
            self.futures.append(future)
            if not future.finished:
                break
            if future.finished and future.exception:
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
