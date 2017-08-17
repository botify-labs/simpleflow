from __future__ import absolute_import, print_function

import os
import sys

import time

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess
import functools
import logging
import tempfile
import traceback

# noinspection PyCompatibility
from builtins import map

from future.utils import iteritems

from swf import format
from simpleflow import compat
from simpleflow.exceptions import ExecutionError, ExecutionTimeoutError
from simpleflow.utils import json_dumps

__all__ = ['program', 'python']


class RequiredArgument(object):
    pass


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

    return ['{}="{}"'.format(arg(k), v) for k, v in
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
    if not argspec.varargs and argspec.args and len(args) != len(argspec.args):
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


def wait_subprocess(process, timeout=None, command_info=None):
    """
    Wait for a process, raise if timeout.
    :param process: the process to wait
    :param timeout: timeout after 'timeout' seconds
    :type timeout: int | None
    :param command_info:
        :returns: return code
        :rtype: int.
    """
    if timeout:
        t_start = time.time()
        rc = process.poll()
        while time.time() - t_start < timeout and rc is None:
            time.sleep(1)
            rc = process.poll()

        if rc is None:
            try:
                process.terminate()  # send SIGTERM
            except OSError as e:
                # Ignore that exception the case the sub-process already terminated after last poll() call.
                if e.errno != 3:
                    raise
            raise ExecutionTimeoutError(command=command_info, timeout_value=timeout)
        return rc
    return process.wait()


def python(interpreter='python', logger_name=__name__, timeout=None):
    """
    Execute a callable as an external Python program.

    One of the use cases is to use a different interpreter than the current one
    such as pypy.

    Arguments of the decorated callable must be serializable in JSON.

    """

    def wrap_callable(func):
        @functools.wraps(func)
        def execute(*args, **kwargs):
            logger = logging.getLogger(logger_name)
            command = 'simpleflow.execute'  # name of a module.
            sys.stdout.flush()
            sys.stderr.flush()
            result_str = None  # useless
            with tempfile.TemporaryFile() as result_fd, tempfile.TemporaryFile() as error_fd:
                dup_result_fd = os.dup(result_fd.fileno())  # remove FD_CLOEXEC
                dup_error_fd = os.dup(error_fd.fileno())  # remove FD_CLOEXEC
                # print('error_fd: {}'.format(dup_error_fd))
                full_command = [
                    interpreter, '-m', command,  # execute module a script.
                    get_name(func), format_arguments_json(*args, **kwargs),
                    '--logger-name={}'.format(logger_name),
                    '--result-fd={}'.format(dup_result_fd),
                    '--error-fd={}'.format(dup_error_fd),
                ]
                if compat.PY2:  # close_fds doesn't work with python2 (using its C _posixsubprocess helper)
                    close_fds = False
                    pass_fds = ()
                else:
                    close_fds = True
                    pass_fds = (dup_result_fd, dup_error_fd)
                process = subprocess.Popen(
                    full_command,
                    bufsize=-1,
                    close_fds=close_fds,
                    pass_fds=pass_fds,
                )

                rc = wait_subprocess(process, timeout=timeout, command_info=full_command)

                os.close(dup_result_fd)
                os.close(dup_error_fd)
                if rc:
                    error_fd.seek(0)
                    err_output = error_fd.read()
                    if err_output:
                        if not compat.PY2:
                            err_output = err_output.decode('utf-8', errors='replace')
                    raise ExecutionError(err_output)

                result_fd.seek(0)
                result_str = result_fd.read()

            if not result_str:
                return None
            try:
                if not compat.PY2:
                    result_str = result_str.decode('utf-8', errors='replace')
                result = format.decode(result_str)
                return result
            except BaseException as ex:
                logger.exception('Exception in python.execute: {} {}'.format(ex.__class__.__name__, ex))
                logger.warning('%r', result_str)

        # Not automatically assigned in python < 3.2.
        execute.__wrapped__ = func
        return execute

    return wrap_callable


def program(path=None, argument_format=format_arguments):
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


def main():
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
    parser.add_argument(
        '--logger-name',
        help='logger name',
    )
    parser.add_argument(
        '--result-fd',
        type=int,
        default=1,
        metavar='N',
        help='result file descriptor',
    )
    parser.add_argument(
        '--error-fd',
        type=int,
        default=2,
        metavar='N',
        help='error file descriptor',
    )
    cmd_arguments = parser.parse_args()
    funcname = cmd_arguments.funcname
    try:
        arguments = format.decode(cmd_arguments.funcargs)
    except:
        raise ValueError('cannot load arguments from {}'.format(
            cmd_arguments.funcargs))
    if cmd_arguments.logger_name:
        logger = logging.getLogger(cmd_arguments.logger_name)
    else:
        logger = logging.getLogger(__name__)
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
    except Exception as err:
        logger.error('Exception: {}'.format(err))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = traceback.format_tb(exc_traceback)
        details = json_dumps(
            {
                'error': exc_type.__name__,
                'message': str(exc_value),
                'traceback': tb,
            },
            default=repr,
        )
        if cmd_arguments.error_fd == 2:
            sys.stderr.flush()
        if not compat.PY2:
            details = details.encode('utf-8')
        os.write(cmd_arguments.error_fd, details)
        sys.exit(1)

    if cmd_arguments.result_fd == 1:  # stdout (legacy)
        sys.stdout.flush()  # may have print's in flight
        os.write(cmd_arguments.result_fd, b'\n')
    result = json_dumps(result)
    if not compat.PY2:
        result = result.encode('utf-8')
    os.write(cmd_arguments.result_fd, result)


if __name__ == '__main__':
    main()
