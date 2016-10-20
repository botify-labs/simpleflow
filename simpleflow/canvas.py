from . import futures
from .activity import Activity
from .task import ActivityTask


class FuncGroup(object):
    """
    Class calling a function returning an ActivityTask, a group or a chain
    activities : Group, Chain...
    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = list(args)
        self.kwargs = kwargs

    def submit(self, executor):
        inst = self.func(*self.args, **self.kwargs)
        if not isinstance(inst, (ActivityTask, Group)):
            raise TypeError('FuncGroup submission should return a Group or an ActivityTask,'
                            ' got {} instead'.format(type(inst)))
        return inst.submit(executor)


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


class Group(object):
    """
    List of activities running in parallel.
    """

    def __init__(self,
                 *activities,
                 **options):
        self.activities = list(activities)

    def append(self, *args, **kwargs):
        if isinstance(args[0], (ActivityTask, Group)):
            self.activities.append(args[0])
        elif isinstance(args[0], Activity):
            self.activities.append(ActivityTask(*args, **kwargs))
        else:
            raise ValueError('{} should be an ActivityTask or an Activity'.format(args[0]))

    def submit(self, executor):
        return GroupFuture(self.activities, executor)


class GroupFuture(futures.Future):

    def __init__(self, activities, executor):
        super(GroupFuture, self).__init__()
        self.activities = activities
        self.futures = []
        self.executor = executor
        for a in self.activities:
            self.futures.append(self._submit_activity(a))
        self.sync_state()
        self.sync_result()

    def _submit_activity(self, act):
        if isinstance(act, ActivityTask):
            return self.executor.submit(act.activity, *act.args, **act.kwargs)
        elif isinstance(act, (Group, FuncGroup)):
            return act.submit(self.executor)
        raise TypeError('Wrong type for `act` ({}). Expecting `ActivityTask`, `Group` or `FuncGroup`'.format(type(act)))

    def sync_state(self):
        if all(a.finished for a in self.futures):
            self._state = futures.FINISHED
        elif any(a.cancelled for a in self.futures):
            self._state = futures.CANCELLED
        elif any(a.running for a in self.futures):
            self._state = futures.RUNNING

    def sync_result(self):
        self._result = []
        exceptions = []
        for future in self.futures:
            if future.finished:
                self._result.append(future.result)
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
            executor,
            self.send_result
        )


class ChainFuture(GroupFuture):
    def __init__(self, activities, executor, send_result=False):
        self.activities = activities
        self.executor = executor
        self._state = futures.PENDING
        self._result = None
        self._exception = None
        self.futures = []

        previous_result = None
        for i, a in enumerate(self.activities):
            if send_result and i > 0:
                a.args.append(previous_result)
            future = self._submit_activity(a)
            self.futures.append(future)
            if not future.finished:
                break
            previous_result = future.result

        self.sync_state()
        self.sync_result()
