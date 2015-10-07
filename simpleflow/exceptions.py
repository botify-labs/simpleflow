class ExecutionBlocked(Exception):
    pass


class TaskException(Exception):
    """
    Wrap an exception raised by a task.

    """
    def __init__(self, task, exception):
        """
        :param exception: raised by a task.
        :type  exception: TaskFailed.

        """
        self.task = task
        self.exception = exception

    def __repr__(self):
        return '{}(task={} exception={})'.format(
            self.__class__.__name__,
            self.task,
            self.exception)


class TaskFailed(Exception):
    """
    Wrap the error's *reason* and *details* for an task that failed.

    :param name: of the task that failed.
    :type name: str.
    :param reason: of the failure.
    :type  reason: str.
    :param details: of the failure.
    :type  details: str.

    """
    def __init__(self, name, reason, details=None):
        super(TaskFailed, self).__init__(name, reason, details)
        self.name = name
        self.reason = reason
        self.details = None

    def __repr__(self):
        return '{} ({}, "{}")'.format(
            self.__class__.__name__,
            self.name,
            self.reason,
        )


class TimeoutError(Exception):
    def __init__(self, timeout_type='unknown timeout', timeout_value=None):
        self.timeout_type = timeout_type
        self.timeout_value = timeout_value

    def __repr__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            self.timeout_type)
