import functools
import inspect
import subprocess

from . import task


__all__ = ['with_attributes', 'Activity']


def with_attributes(name=None, version=None, task_list=None,
                    retry=0,
                    raises_on_failure=False,
                    start_to_close_timeout=None,
                    schedule_to_close_timeout=None,
                    schedule_to_start_timeout=None,
                    heartbeat_timeout=None):
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
            heartbeat_timeout
        )

    return wrap


class RequiredArgument(object):
    pass


def format_arguments(*args, **kwargs):
    """
    Examples:

        >>> format_arguments('a', 'b', c=1, d=2)
        ['--c=1', '--d=2', 'a', 'b']

    """
    return ['--{}="{}"'.format(key, value) for key, value in
            kwargs.iteritems()] + map(str, args)


def zip_arguments_defaults(argspec):
    if not argspec.defaults:
        return []

    return zip(
        argspec.args[-len(argspec.defaults):],
        argspec.defaults)


def check_arguments(argspec, args):
    # func() or func(**kwargs) or func(a=1, b=2)
    if not argspec.varargs and not argspec.args and args:
        raise TypeError('command does not take varargs')

    # Calling func(a, b) with func(1, 2, 3)
    if (not argspec.varargs and argspec.args and
            len(args) != len(argspec.args)):
        raise TypeError('command takes {} arguments: {} passed'.format(
            len(argspec.args),
            len(args)))


def check_keyword_arguments(argspec, kwargs):
    # func() or func(*args) or func(a, b)
    if not argspec.keywords and not argspec.defaults and kwargs:
        raise TypeError('command does not take keyword arguments')

    arguments_defaults = zip_arguments_defaults(argspec)
    not_found = (set(name for name, value in arguments_defaults if
                     value is RequiredArgument) -
                 set(kwargs))
    # Calling func(a=1, b) with func(2) instead of func(a=0, 2)
    if not_found:
        raise TypeError('argument{} "{}" not found'.format(
            's' if len(not_found) > 1 else '',
            ', '.join(not_found)))


def execute_program(path=None, argument_format=format_arguments):
    def wrap_callable(func):
        @functools.wraps(func)
        def execute(*args, **kwargs):
            check_arguments(argspec, args)
            check_keyword_arguments(argspec, kwargs)

            command = path or func.func_name
            return subprocess.check_output(
                [command] + argument_format(*args, **kwargs))

        argspec = inspect.getargspec(func)
        return execute
    return wrap_callable


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
                 heartbeat_timeout=None):
        self._callable = callable

        self._name = name
        self.version = version
        self.task_list = task_list
        self.retry = retry
        self.raises_on_failure = raises_on_failure
        self.task_start_to_close_timeout = start_to_close_timeout
        self.task_schedule_to_close_timeout = schedule_to_close_timeout
        self.task_schedule_to_start_timeout = schedule_to_start_timeout
        self.task_heartbeat_timeout = heartbeat_timeout

        self.register()

    def register(self):
        task.registry.register(self, self.task_list)

    @property
    def name(self):
        import types

        if self._name is not None:
            return self._name

        callable = self._callable
        prefix = self._callable.__module__

        if hasattr(callable, 'name'):
            name = callable.name
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
