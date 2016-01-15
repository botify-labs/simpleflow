from . import settings
from . import registry


__all__ = ['with_attributes', 'Activity']


def with_attributes(
        name=None,
        version=settings.ACTIVITY_DEFAULT_VERSION,
        task_list=settings.ACTIVITY_DEFAULT_TASK_LIST,
        retry=0,
        raises_on_failure=False,
        start_to_close_timeout=settings.ACTIVITY_START_TO_CLOSE_TIMEOUT,
        schedule_to_close_timeout=settings.ACTIVITY_SCHEDULE_TO_CLOSE_TIMEOUT,
        schedule_to_start_timeout=settings.ACTIVITY_SCHEDULE_TO_START_TIMEOUT,
        heartbeat_timeout=settings.ACTIVITY_HEARTBEAT_TIMEOUT,
        idempotent=None,
):
    """
    :param name: of the activity type.
    :type  name: str.

    """
    def wrap(func):
        return Activity(
            func, name, version, task_list,
            retry,
            raises_on_failure,
            start_to_close_timeout,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            heartbeat_timeout,
            idempotent=idempotent,
        )

    return wrap


class Activity(object):
    def __init__(self, callable,
                 name=None,
                 version=None,
                 task_list=None,
                 retry=0,
                 raises_on_failure=False,
                 start_to_close_timeout=None,
                 schedule_to_close_timeout=None,
                 schedule_to_start_timeout=None,
                 heartbeat_timeout=None,
                 idempotent=None,
                 activity_completed=None):
        self._callable = callable

        self._name = name
        self.version = version
        self.task_list = task_list
        self.retry = retry
        self.raises_on_failure = raises_on_failure
        self.idempotent = idempotent
        self.task_start_to_close_timeout = start_to_close_timeout
        self.task_schedule_to_close_timeout = schedule_to_close_timeout
        self.task_schedule_to_start_timeout = schedule_to_start_timeout
        self.task_heartbeat_timeout = heartbeat_timeout
        self.activity_completed = self.activity_completed if activity_completed is None else activity_completed

        self.register()

    def register(self):
        registry.registry.register(self)

    @property
    def name(self):
        import types

        if self._name is not None:
            return self._name

        callable = self._callable
        prefix = self._callable.__module__

        if hasattr(callable, 'name'):
            name = callable.name
        elif hasattr(callable, '__name__'):
            name = callable.__name__
        elif isinstance(callable, types.FunctionType):
            name = callable.func_name
        else:
            name = callable.__class__.__name__

        return '.'.join([prefix, name])

    def __repr__(self):
        return 'Activity(name={}, version={}, task_list={})'.format(
            self.name,
            self.version,
            self.task_list)

    def before_scheduling(*args, **kwargs):
        pass

    def after_scheduling(*args, **kwargs):
        pass

    def activity_started(*args, **kwargs):
        pass

    def activity_completed(*args, **kwargs):
        pass

    def activity_canceled(*args, **kwargs):
        pass

    def activity_failed(*args, **kwargs):
        pass

    def activity_timedout(*args, **kwargs):
        pass
