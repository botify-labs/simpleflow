import collections


class Registry(object):
    def __init__(self):
        self._tasks = collections.defaultdict(dict)

    def __getitem__(self, label):
        return self._tasks[label]

    def register(self, task, label=None):
        self._tasks[label][task.name] = task

    def execute(self):
        raise NotImplementedError()

registry = Registry()
