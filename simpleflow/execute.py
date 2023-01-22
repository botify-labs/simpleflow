from __future__ import annotations

import errno
import functools
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
import traceback
from typing import TYPE_CHECKING

import psutil

from simpleflow import format
from simpleflow import logger as simpleflow_logger
from simpleflow.exceptions import ExecutionError, ExecutionTimeoutError
from simpleflow.utils import import_from_module, json_dumps

if TYPE_CHECKING:
    import inspect
    from typing import Any, Iterable


MAX_ARGUMENTS_JSON_LENGTH = 65536


__all__ = ["program", "python"]


class RequiredArgument:
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
            return "-" + str(key)  # short option -c
        return "--" + str(key)  # long option --val

    return [f'{arg(k)}="{v}"' for k, v in kwargs.items()] + list(map(str, args))


def zip_arguments_defaults(argspec: inspect.ArgSpec) -> Iterable:
    if not argspec.defaults:
        return []

    return list(zip(argspec.args[-len(argspec.defaults) :], argspec.defaults))


def check_arguments(argspec: inspect.ArgSpec, args: Any) -> None:
    """Validates there is the right number of arguments"""
    # func() or func(**kwargs) or func(a=1, b=2)
    if not argspec.varargs and not argspec.args and args:
        raise TypeError("command does not take varargs")

    # Calling func(a, b) with func(1, 2, 3)
    if not argspec.varargs and argspec.args and len(args) != len(argspec.args):
        raise TypeError(f"command takes {len(argspec.args)} arguments: {len(args)} passed")


def check_keyword_arguments(argspec: inspect.ArgSpec, kwargs: dict) -> None:
    # func() or func(*args) or func(a, b)
    if not argspec.keywords and not argspec.defaults and kwargs:
        raise TypeError("command does not take keyword arguments")

    arguments_defaults = zip_arguments_defaults(argspec)
    not_found = {name for name, value in arguments_defaults if value is RequiredArgument} - set(kwargs)
    # Calling func(a=1, b) with func(2) instead of func(a=0, 2)
    if not_found:
        raise TypeError('argument{} "{}" not found'.format("s" if len(not_found) > 1 else "", ", ".join(not_found)))


def format_arguments_json(*args, **kwargs):
    dump = json_dumps(
        {
            "args": args,
            "kwargs": kwargs,
        }
    )
    return dump


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
        raise ValueError(f"{func} is not callable")

    if hasattr(func, "name"):
        name = func.name
    elif hasattr(func, "__name__"):
        name = func.__name__
    else:
        name = func.__class__.__name__

    return ".".join([prefix, name])


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
                if e.errno == errno.ESRCH:
                    return process.poll()
                else:
                    raise
            raise ExecutionTimeoutError(command=command_info, timeout_value=timeout)
        return rc
    return process.wait()


def python(
    interpreter: str = "python",
    logger_name: str = __name__,
    timeout: int | None = None,
    kill_children: bool = False,
    env: dict | None = None,
):
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
            command = "simpleflow.execute"  # name of a module.
            sys.stdout.flush()
            sys.stderr.flush()
            result_str = None  # useless
            context = kwargs.pop("context", {})
            tmp_dir = None
            if env:
                for envname in "TMPDIR", "TEMP", "TMP":
                    tmp_dir = env.get(envname)
                    if tmp_dir:
                        break
            with tempfile.TemporaryFile(dir=tmp_dir) as result_fd, tempfile.TemporaryFile(dir=tmp_dir) as error_fd:
                dup_result_fd = os.dup(result_fd.fileno())  # remove FD_CLOEXEC
                dup_error_fd = os.dup(error_fd.fileno())  # remove FD_CLOEXEC
                arguments_json = format_arguments_json(*args, **kwargs)
                full_command = [
                    interpreter,
                    "-m",
                    command,  # execute module a script.
                    get_name(func),
                    f"--logger-name={logger_name}",
                    f"--result-fd={dup_result_fd}",
                    f"--error-fd={dup_error_fd}",
                    f"--context={json_dumps(context)}",
                ]
                if len(arguments_json) < MAX_ARGUMENTS_JSON_LENGTH:  # command-line limit on Linux: 128K
                    full_command.append(arguments_json)
                    arg_file = None
                    arg_fd = None
                else:
                    arg_file = tempfile.TemporaryFile(dir=tmp_dir)
                    arg_file.write(arguments_json.encode("utf-8"))
                    arg_file.flush()
                    arg_file.seek(0)
                    arg_fd = os.dup(arg_file.fileno())
                    full_command.append(f"--arguments-json-fd={arg_fd}")
                    full_command.append("foo")  # dummy funcarg
                if kill_children:
                    full_command.append("--kill-children")
                close_fds = True
                pass_fds = [dup_result_fd, dup_error_fd]
                if arg_file:
                    pass_fds.append(arg_fd)
                process = subprocess.Popen(  # nosec
                    full_command,
                    bufsize=-1,
                    close_fds=close_fds,
                    pass_fds=pass_fds,
                    env=env,
                )
                rc = wait_subprocess(process, timeout=timeout, command_info=full_command)
                os.close(dup_result_fd)
                os.close(dup_error_fd)
                if arg_file:
                    arg_file.close()
                if rc:
                    error_fd.seek(0)
                    err_output = error_fd.read()
                    if err_output:
                        err_output = err_output.decode("utf-8", errors="replace")
                    raise ExecutionError(err_output)

                result_fd.seek(0)
                result_str = result_fd.read()

            if not result_str:
                return None
            try:
                result_str = result_str.decode("utf-8", errors="replace")
                result = format.decode(result_str)
                return result
            except BaseException as ex:
                logger.exception("Exception in python.execute: {} {}".format(ex.__class__.__name__, ex))
                logger.warning("%r", result_str)

        # Not automatically assigned in python < 3.2.
        execute.__wrapped__ = func
        execute.add_context_in_kwargs = True
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
            return subprocess.check_output([command] + argument_format(*args, **kwargs), text=True)  # nosec

        try:
            (args, varargs, varkw, defaults, kwonlyargs, kwonlydefaults, ann,) = inspect.getfullargspec(  # noqa
                func
            )
            argspec = inspect.ArgSpec(args, varargs, varkw, defaults)
        except AttributeError:
            # noinspection PyDeprecation
            argspec = inspect.getargspec(func)

        # Not automatically assigned in python < 3.2.
        execute.__wrapped__ = func
        return execute

    return wrap_callable


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
        "funcname",
        help="name of the callable to execute",
    )
    parser.add_argument(
        "funcargs",
        help="callable arguments in JSON",
    )
    parser.add_argument(
        "--context",
        help="Activity Context",
    )
    parser.add_argument(
        "--logger-name",
        help="logger name",
    )
    parser.add_argument(
        "--result-fd",
        type=int,
        default=1,
        metavar="N",
        help="result file descriptor",
    )
    parser.add_argument(
        "--error-fd",
        type=int,
        default=2,
        metavar="N",
        help="error file descriptor",
    )
    parser.add_argument(
        "--arguments-json-fd",
        type=int,
        default=None,
        metavar="N",
        help="JSON input file descriptor",
    )
    parser.add_argument(
        "--kill-children",
        action="store_true",
        help="kill child processes on exit",
    )
    cmd_arguments = parser.parse_args()

    def kill_child_processes():
        process = psutil.Process(os.getpid())
        children = process.children(recursive=True)

        for child in children:
            try:
                child.terminate()
            except psutil.NoSuchProcess:
                pass
        _, still_alive = psutil.wait_procs(children, timeout=0.3)
        for child in still_alive:
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass

    funcname = cmd_arguments.funcname
    if cmd_arguments.arguments_json_fd is None:
        content = cmd_arguments.funcargs
        if content is None:
            parser.error("the following arguments are required: funcargs")
    else:
        with os.fdopen(cmd_arguments.arguments_json_fd) as arguments_json_file:
            content = arguments_json_file.read()
    try:
        arguments = format.decode(content)
    except Exception:
        raise ValueError(f"cannot load arguments from {content}")
    if cmd_arguments.logger_name:
        logger = logging.getLogger(cmd_arguments.logger_name)
    else:
        logger = simpleflow_logger
    callable_ = import_from_module(funcname)
    if hasattr(callable_, "__wrapped__"):
        callable_ = callable_.__wrapped__
    args = arguments.get("args", ())
    kwargs = arguments.get("kwargs", {})
    context = json.loads(cmd_arguments.context) if cmd_arguments.context is not None else None
    try:
        if hasattr(callable_, "execute"):
            inst = callable_(*args, **kwargs)
            if context is not None:
                inst.context = context
            result = inst.execute()
            if hasattr(inst, "post_execute"):
                inst.post_execute()
        else:
            if context is not None:
                callable_.context = context
            result = callable_(*args, **kwargs)
    except Exception as err:
        logger.error(f"Exception: {err}")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = traceback.format_tb(exc_traceback)
        details = json_dumps(
            {
                "error": exc_type.__name__,
                "message": str(exc_value),
                "traceback": tb,
            },
            default=repr,
        )
        if cmd_arguments.error_fd == 2:
            sys.stderr.flush()
        details = details.encode("utf-8")
        os.write(cmd_arguments.error_fd, details)
        if cmd_arguments.kill_children:
            kill_child_processes()
        sys.exit(1)

    if cmd_arguments.result_fd == 1:  # stdout (legacy)
        sys.stdout.flush()  # may have print's in flight
        os.write(cmd_arguments.result_fd, b"\n")
    result = json_dumps(result)
    result = result.encode("utf-8")
    os.write(cmd_arguments.result_fd, result)
    if cmd_arguments.kill_children:
        kill_child_processes()


if __name__ == "__main__":
    main()
