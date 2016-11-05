from __future__ import absolute_import, print_function

import os
import sys
import functools
import json

from simpleflow.utils.subprocess_utils import communicate_with_limits

from builtins import map

from future.utils import iteritems
import logging

from simpleflow.utils import json_dumps

if os.name == 'posix' and sys.version_info[0] < 3:
    try:
        import subprocess32 as subprocess
    except ImportError:
        import subprocess
else:
    import subprocess

__all__ = ['program', 'python']

logger = logging.getLogger(__name__)


class RequiredArgument(object):
    pass


class ExecuteError(Exception):
    """
    Exception raised by `python`.
    """
    def __init__(self, exc_type, traceback, *args, **kwargs):
        super(ExecuteError, self).__init__(*args, **kwargs)
        self.exc_type = exc_type
        self.traceback = traceback


def format_arguments(*args, **kwargs):
    """
    Returns a string that contains the values of *args* and *kwargs* as command
    line options.

    :param args: that can be converted to strings.
    :type  args: tuple.
    :param kwargs: whose keys and values can be converted to strings.
    :type  kwargs: dict.

    :returns:
        :rtype: str.

    The elements args must be convertible to strings and will be used as
    positional arguments.

    The items of *kwargs* are translated to key/value options (-c=1). Their
    format follows the convention of one hyphen for short options (-c) and two
    hyphens for long options (--val).

    Examples:

        >>> sorted(format_arguments('a', 'b', c=1, val=2))
        ['--val="2"', '-c="1"', 'a', 'b']

    """

    def arg(key):
        if len(key) == 1:
            return '-' + str(key)  # short option -c
        return '--' + str(key)  # long option --val

    return ['{}="{}"'.format(arg(key), value) for key, value in
            iteritems(kwargs)] + list(map(str, args))


def zip_arguments_defaults(argspec):
    if not argspec.defaults:
        return []

    return zip(
        argspec.args[-len(argspec.defaults):],
        argspec.defaults)


def check_arguments(argspec, args):
    """Validates there is the right number of arguments"""
    # func() or func(**kwargs) or func(a=1, b=2)
    if not argspec.varargs and not argspec.args and args:
        raise TypeError('command does not take varargs')

    # Calling func(a, b) with func(1, 2, 3)
    if (not argspec.varargs and argspec.args and len(args) != len(argspec.args)):
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


def format_arguments_json(*args, **kwargs):
    return json_dumps({
        'args': args,
        'kwargs': kwargs,
    })


def get_name(func):
    """
    Returns the name of a callable.

    It handles different types of callable: function, callable object with
    ``__call__`` method and callable objects that provide their name in the
    ``name`` attributes.

    :type func: callable.
    :returns:
        :rtype: str.

    """
    prefix = func.__module__

    if not callable(func):
        raise ValueError('{} is not callable'.format(
            func))

    if hasattr(func, 'name'):
        name = func.name
    elif hasattr(func, '__name__'):
        name = func.__name__
    else:
        name = func.__class__.__name__

    return '.'.join([prefix, name])


def python(interpreter='python'):
    """
    Execute a callable as an external Python program.

    One of the use cases is to use a different interpreter than the current one
    such as pypy.

    Arguments of the decorated callable must be serializable in JSON.

    """

    def wrap_callable(func):
        @functools.wraps(func)
        def execute(*args, **kwargs):
            command = 'simpleflow.execute'  # name of a module.
            full_command = [
                interpreter, '-m', command,  # execute module a script.
                get_name(func), format_arguments_json(*args, **kwargs),
            ]
            try:
                proc = subprocess.Popen(full_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            except Exception:
                logger.exception('Exception on Popen({})'.format(full_command))
                raise
            try:
                stdout, stderr = communicate_with_limits(proc, stdout_limit=32768, stderr_limit=32768)
            except Exception:
                logger.exception('Exception on communicate({})'.format(full_command))
                raise
            if proc.returncode != 0:
                logger.warning('Non-zero return code {}; output:\n{}\n'.format(proc.returncode, stderr))
                last_line = stderr.rstrip().rsplit('\n', 1)[-1]
                try:
                    err_dict = json.loads(last_line)
                except Exception:
                    logger.warning('No JSON object on last line?', exc_info=True)
                    err_dict = {}
                raise ExecuteError(
                    exc_type=err_dict.get('exc_type'), traceback=err_dict.get('traceback'),
                    args=err_dict.get('args')
                )
            last_line = stdout.rstrip().rsplit('\n', 1)[-1]
            try:
                rc = json.load(last_line)
            except Exception:
                logger.warning('No JSON object on last line?', exc_info=True)
                rc = None
            return rc

        # Not automatically assigned in python < 3.2.
        execute.__wrapped__ = func
        return execute

    return wrap_callable


def program(path=None, argument_format=format_arguments):
    # FIXME use communicate?
    r"""
    Decorate a callable to execute it as an external program.

    :param path: of the program to execute. If it is ``None`` the name of the
                 executable will be the name of the callable.
    :type  path: str.
    :param argument_format: takes the arguments of the callable and converts
                            them to command line arguments.
    :type  argument_format: callable(*args, **kwargs).

    :returns:
        :rtype: callable(*args, **kwargs).

    Examples
    --------

    >>> @program()
    ... def ls(path):
    ...     pass
    >>> ls('/etc/resolv.conf')
    '/etc/resolv.conf\n'

    It will execute the ``ls`` command and requires a single positional
    argument *path*.

    """
    import inspect

    def wrap_callable(func):
        @functools.wraps(func)
        def execute(*args, **kwargs):
            check_arguments(argspec, args)
            check_keyword_arguments(argspec, kwargs)

            command = path or func.__name__
            return subprocess.check_output(
                [command] + argument_format(*args, **kwargs),
                universal_newlines=True)

        argspec = inspect.getargspec(func)
        # Not automatically assigned in python < 3.2.
        execute.__wrapped__ = func
        return execute

    return wrap_callable


def make_callable(funcname):
    """
    Return a callable object from a string.

    This function resolves a name into a callable object. It automatically
    loads the required modules. If there is no module path, it considers the
    callable is a builtin.

    :param funcname: name of the callable.
    :type  funcname: str.

    :returns:
        :rtype: callable.

    Examples
    --------

    Loading a function from a library:

    >>> func = make_callable('itertools.chain')
    >>> list(func(range(3), range(4)))
    [0, 1, 2, 0, 1, 2, 3]

    Loading a builtin:

    >>> func = make_callable('map')
    >>> list(func(lambda x: x + 1, range(4)))
    [1, 2, 3, 4]

    """
    if '.' not in funcname:
        module_name = 'builtins'
        object_name = funcname
    else:
        module_name, object_name = funcname.rsplit('.', 1)

    module = __import__(module_name, fromlist=['*'])
    try:
        callable_ = getattr(module, object_name)
    except AttributeError:
        raise AttributeError('module {} has no attribute {}'.format(
            module.__name__,
            object_name,
        ))
    return callable_


if __name__ == '__main__':
    """
    When executed as a script, this module expects the name of a callable as
    its first argument and the arguments of the callable encoded in a JSON
    string as its second argument. It then executes the callable with the
    arguments after decoding them into Python objects. It finally encodes the
    value returned by the callable into a JSON string and prints it on stdout.

    the arguments of the callable are stored in a dict with the following
    format: ::

        {'args': [...],
         'kwargs': {
            ...,
         }
         }

    Synopsis
    --------

    ::
        usage: execute.py [-h] funcname funcargs

        positional arguments:
          funcname    name of the callable to execute
          funcargs    callable arguments in JSON

        optional arguments:
          -h, --help  show this help message and exit

    Examples
    --------

    ::
        $ python -m simpleflow.execute "os.path.exists" '{"args": ["/tmp"]}'
        true

    """
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'funcname',
        help='name of the callable to execute',
    )
    parser.add_argument(
        'funcargs',
        help='callable arguments in JSON',
    )

    cmd_arguments = parser.parse_args()

    funcname = cmd_arguments.funcname
    try:
        arguments = json.loads(cmd_arguments.funcargs)
    except:
        raise ValueError('cannot load arguments from {}'.format(
            cmd_arguments.funcargs))

    callable_ = make_callable(funcname)
    if hasattr(callable_, '__wrapped__'):
        callable_ = callable_.__wrapped__

    args = arguments.get('args', ())
    kwargs = arguments.get('kwargs', {})
    try:
        if hasattr(callable_, 'execute'):
            result = callable_(*args, **kwargs).execute()
        else:
            result = callable_(*args, **kwargs)
    except Exception as exc:
        import traceback
        tb = traceback.format_exc()
        logger.exception('Error calling {}'.format(funcname))
        err_dict = dict(
            exc_type=exc.__class__.__name__,
            traceback=tb,
            args=exc.args,
        )
        print(json_dumps(err_dict), file=sys.stderr)
        sys.exit(1)
    else:
        print(json_dumps(result))
