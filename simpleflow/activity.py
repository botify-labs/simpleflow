from __future__ import annotations

from typing import TYPE_CHECKING

from . import registry, settings

if TYPE_CHECKING:
    from typing import Callable


__all__ = [
    "with_attributes",
    "Activity",
    "PRIORITY_NOT_SET",
]


class NotSet:
    def __repr__(self):
        return "<Priority Not Set>"


PRIORITY_NOT_SET = NotSet()


def with_attributes(
    name=None,
    version=settings.ACTIVITY_DEFAULT_VERSION,
    task_list=settings.ACTIVITY_DEFAULT_TASK_LIST,
    task_priority=PRIORITY_NOT_SET,
    retry=0,
    raises_on_failure=False,
    start_to_close_timeout=settings.ACTIVITY_START_TO_CLOSE_TIMEOUT,
    schedule_to_close_timeout=settings.ACTIVITY_SCHEDULE_TO_CLOSE_TIMEOUT,
    schedule_to_start_timeout=settings.ACTIVITY_SCHEDULE_TO_START_TIMEOUT,
    heartbeat_timeout=settings.ACTIVITY_HEARTBEAT_TIMEOUT,
    idempotent=None,
    meta=None,
) -> Callable[[Callable], Activity]:
    """
    Decorator: wrap a function/class into an Activity.

    :param name: name of the activity.
    :type  name: str.
    :param version: optional version.
    :type version: str
    :param task_list: optional task list.
    :type task_list: str
    :param retry: retry count.
    :type retry: int
    :param raises_on_failure: whether to raise on failure.
    :type raises_on_failure: bool
    :param start_to_close_timeout:
    :type start_to_close_timeout: str | int
    :param schedule_to_close_timeout:
    :type schedule_to_close_timeout: str | int
    :param schedule_to_start_timeout:
    :type schedule_to_start_timeout: str | int
    :param heartbeat_timeout:
    :type heartbeat_timeout: str | int
    :param idempotent: True if the activity is idempotent.
    :type idempotent: Optional[bool]
    :param meta:
    :type meta: str
    :rtype: () -> Activity[()]

    """

    def wrap(func: Callable) -> Activity:
        return Activity(
            func,
            name,
            version,
            task_list,
            retry,
            raises_on_failure,
            start_to_close_timeout,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            heartbeat_timeout,
            task_priority=task_priority,
            idempotent=idempotent,
            meta=meta,
        )

    return wrap


class Activity:
    def __init__(
        self,
        callable,
        name=None,
        version=None,
        task_list=None,
        retry=0,
        raises_on_failure=False,
        start_to_close_timeout=None,
        schedule_to_close_timeout=None,
        schedule_to_start_timeout=None,
        heartbeat_timeout=None,
        task_priority=PRIORITY_NOT_SET,
        idempotent=None,
        meta=None,
    ):
        self._callable = callable

        self._name = name
        self.version = version
        self.task_list = task_list
        self.task_priority = task_priority
        self.retry = retry
        self.raises_on_failure = raises_on_failure
        self.idempotent = idempotent
        self.task_start_to_close_timeout = start_to_close_timeout
        self.task_schedule_to_close_timeout = schedule_to_close_timeout
        self.task_schedule_to_start_timeout = schedule_to_start_timeout
        self.task_heartbeat_timeout = heartbeat_timeout
        self.meta = meta if meta is not None else {}

        self.register()

    def register(self):
        registry.registry.register(self)

    @property
    def callable(self):
        return self._callable

    @property
    def context(self):
        return getattr(self.callable, "context", None)

    @property
    def name(self):
        if self._name is not None:
            return self._name

        callable = self._callable
        prefix = self._callable.__module__

        if hasattr(callable, "name"):
            name = callable.name
        elif hasattr(callable, "__name__"):
            name = callable.__name__
        else:
            name = callable.__class__.__name__

        return ".".join([prefix, name])

    def __repr__(self):
        return "Activity(name={}, version={}, task_list={})".format(self.name, self.version, self.task_list)

    def propagate_attribute(self, attr, val):
        setattr(self, attr, val)
