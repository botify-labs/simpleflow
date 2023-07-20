from __future__ import annotations

from typing import TYPE_CHECKING

from . import registry, settings

if TYPE_CHECKING:
    from typing import Any, Callable


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
    name: str | None = None,
    version: str = settings.ACTIVITY_DEFAULT_VERSION,
    task_list: str = settings.ACTIVITY_DEFAULT_TASK_LIST,
    task_priority: str | NotSet = PRIORITY_NOT_SET,
    retry: int | Any = 0,
    raises_on_failure: bool = False,
    start_to_close_timeout: int | str | None = settings.ACTIVITY_START_TO_CLOSE_TIMEOUT,
    schedule_to_close_timeout: int | str | None = settings.ACTIVITY_SCHEDULE_TO_CLOSE_TIMEOUT,
    schedule_to_start_timeout: int | str | None = settings.ACTIVITY_SCHEDULE_TO_START_TIMEOUT,
    heartbeat_timeout: int | str | None = settings.ACTIVITY_HEARTBEAT_TIMEOUT,
    idempotent: bool | None = None,
    meta: dict[str, Any] | str | None = None,
) -> Callable[[Callable], Activity]:
    """
    Decorator: wrap a function/class into an Activity.

    :param name: name of the activity.
    :param version: optional version.
    :param task_list: optional task list.
    :param task_priority: optional priority.
    :param retry: retry count.
    :param raises_on_failure: whether to raise on failure.
    :param start_to_close_timeout:
    :param schedule_to_close_timeout:
    :param schedule_to_start_timeout:
    :param heartbeat_timeout:
    :param idempotent: True if the activity is idempotent.
    :param meta:

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
        callable: Callable | type,
        name: str | None = None,
        version: str | None = None,
        task_list: str | None = None,
        retry: int | Any = 0,
        raises_on_failure: bool = False,
        start_to_close_timeout: int | str | None = None,
        schedule_to_close_timeout: int | str | None = None,
        schedule_to_start_timeout: int | str | None = None,
        heartbeat_timeout: int | str | None = None,
        task_priority: str | NotSet = PRIORITY_NOT_SET,
        idempotent: bool | None = None,
        meta: dict[str, Any] | str | None = None,
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
        return f"Activity(name={self.name}, version={self.version}, task_list={self.task_list})"

    def propagate_attribute(self, attr, val):
        setattr(self, attr, val)
