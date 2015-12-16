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
            raise TypeError('FuncGroup submission should return a Group or an ActivityTask, Got {} instead'.format(type(inst)))
        return inst.submit(executor)


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
        raise TypeError('Bad type for `act` ({}). Waiting for `ActivityTask`, `Group` or `FuncGroup` instead'.format(type(act)))

    def sync_state(self):
        if all(a.finished for a in self.futures):
            self._state = futures.FINISHED
        elif any(a.cancelled for a in self.futures):
            self._state = futures.CANCELLED
        elif any(a.running for a in self.futures):
            self._state = futures.RUNNING

    def sync_result(self):
        self._result = []
        for future in self.futures:
            if future.finished:
                self._result.append(future.result)
            else:
                self._result.append(None)

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
