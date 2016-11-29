import collections


class Registry(object):
    """
    simpleflow.activity.Activity register.

    :type _tasks: collections.defaultdict[Any, dict[str, simpleflow.activity.Activity]]
    """
    def __init__(self):
        self._tasks = collections.defaultdict(dict)

    def __getitem__(self, label):
        return self._tasks[label]

    def register(self, task, label=None):
        """
        Register a simpleflow.activity.Activity.
        :param task:
        :type task: simpleflow.activity.Activity
        :param label:
        :type label: Optional[str]
        """
        self._tasks[label][task.name] = task


registry = Registry()
