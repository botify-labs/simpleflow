from __future__ import annotations

import json
import os.path
import platform
import subprocess
import sys
import tempfile
import threading
import time

import psutil
import pytest

from simpleflow import execute
from simpleflow.exceptions import ExecutionError, ExecutionTimeoutError


@execute.program(path="ls")
def ls_nokwargs(*args):
    """
    Only accepts a variable number of positional arguments.

    """
    pass


def test_execute_program_no_kwargs():
    with tempfile.NamedTemporaryFile() as f:
        with pytest.raises(TypeError) as exc_info:
            ls_nokwargs(hide=f.name)

        assert exc_info.value.args[0] == "command does not take keyword arguments"


@execute.program(path="ls")
def ls_noargs(**kwargs):
    """
    Only accepts a variable number of keyword arguments.

    """
    pass


def test_execute_program_no_args():
    with tempfile.NamedTemporaryFile() as f:
        with pytest.raises(TypeError) as exc_info:
            ls_noargs(f.name)

        assert exc_info.value.args[0] == "command does not take varargs"


@execute.program(path="ls")
def ls_restrict_named_arguments(hide=execute.RequiredArgument, *args):
    pass


def test_execute_program_restrict_named_arguments():
    with tempfile.NamedTemporaryFile() as f:
        with pytest.raises(TypeError) as exc_info:
            ls_restrict_named_arguments(f.name)

        assert exc_info.value.args[0] == 'argument "hide" not found'


@execute.program(path="ls")
def ls_optional_named_arguments(hide="", *args):
    pass


@pytest.mark.xfail(platform.system() == "Darwin", reason="ls doesn't have a --hide option on MacOSX")
def test_execute_program_optional_named_arguments():
    with tempfile.NamedTemporaryFile(suffix="\xe9") as f:
        assert ls_optional_named_arguments(f.name).strip() == f.name
        assert f.name not in ls_optional_named_arguments(hide=f.name)


@execute.program()
def ls(*args, **kwargs):
    pass


def test_execute_program_with_positional_arguments():
    with tempfile.NamedTemporaryFile() as f:
        assert ls(f.name).strip() == f.name


@pytest.mark.xfail(platform.system() == "Darwin", reason="ls doesn't have a --hide option on MacOSX")
def test_execute_program_with_named_arguments():
    with tempfile.NamedTemporaryFile() as f:
        assert f.name not in (ls(os.path.dirname(f.name), hide=f.name).strip())


@execute.program()
def ls_2args(a, b):
    pass


def test_ls_2args():
    with pytest.raises(TypeError) as exc_info:
        ls_2args(1, 2, 3)

    assert exc_info.value.args[0] == "command takes 2 arguments: 3 passed"


@execute.python()
def inc(xs):
    return [x + 1 for x in xs]


def test_function_as_program():
    assert inc([1, 2, 3]) == [2, 3, 4]


@execute.python()
def add(a, b=1):
    return a + b


@execute.python()
class Add:
    def __init__(self, a, b=1):
        self.a = a
        self.b = b

    def execute(self):
        return self.a + self.b


def test_function_as_program_with_default_kwarg():
    assert add(4) == 5
    assert Add(4) == 5


def test_function_as_program_with_kwargs():
    assert add(3, 7) == 10
    assert Add(3, 7) == 10


def test_function_as_program_raises_builtin_exception():
    with pytest.raises(ExecutionError) as excinfo:
        add("1")
    assert '"error":"TypeError"' in str(excinfo.value)
    with pytest.raises(ExecutionError) as excinfo:
        Add("1")
    assert '"error":"TypeError"' in str(excinfo.value)


@execute.python()
def print_string(s, retval):
    print(s, end="")
    return retval


@execute.python()
class PrintString:
    def __init__(self, s, retval):
        self.s = s
        self.retval = retval

    def execute(self):
        print(self.s)
        return self.retval


def test_function_with_print():
    actual = print_string("This isn't part of the return value", None)
    assert actual is None, actual
    actual = PrintString("This isn't part of the return value", None)
    assert actual is None, actual


def test_function_with_print_and_return():
    assert print_string("This isn't part of the return value", 42) == 42
    assert PrintString("This isn't part of the return value", 42) == 42


def test_function_returning_lf():
    assert print_string("This isn't part of the return value", "a\nb") == "a\nb"
    assert PrintString("This isn't part of the return value", "a\nb") == "a\nb"


class DummyException(Exception):
    pass


@execute.python()
def raise_dummy_exception():
    raise DummyException


@execute.python()
class RaiseDummyException:
    def __init__(self):
        pass

    @staticmethod
    def execute():
        raise DummyException


def test_function_as_program_raises_custom_exception():
    with pytest.raises(ExecutionError) as excinfo:
        raise_dummy_exception()
    assert '"error":"DummyException"' in str(excinfo.value)
    with pytest.raises(ExecutionError) as excinfo:
        RaiseDummyException()
    assert '"error":"DummyException"' in str(excinfo.value)


@execute.python()
def raise_timeout_error():
    from simpleflow.exceptions import TimeoutError

    raise TimeoutError("timeout", 1)


def test_function_as_program_raises_module_exception():
    with pytest.raises(ExecutionError) as excinfo:
        raise_timeout_error()
    assert '"error":"TimeoutError"' in str(excinfo.value)


@execute.python()
def warn():
    import warnings

    warnings.warn(
        "The _posixsubprocess module is not being used. "
        "Child process reliability may suffer if your "
        "program uses threads.",
        RuntimeWarning,
    )
    raise Exception("Fake Exception")


def test_function_with_warning():
    try:
        warn()
    except Exception:
        pass
    else:
        assert False


def test_function_returning_unicode():
    assert print_string("", "ʘ‿ʘ") == "ʘ‿ʘ"


@execute.python()
def raise_dummy_exception_with_unicode():
    raise DummyException("ʘ‿ʘ")


def test_exception_with_unicode():
    with pytest.raises(ExecutionError) as excinfo:
        raise_dummy_exception_with_unicode()
    assert '"error":"DummyException"' in str(excinfo.value)
    error = json.loads(excinfo.value.args[0])
    assert error["message"] == "ʘ‿ʘ"


def sleep_and_return(seconds):
    time.sleep(seconds)
    return seconds


def test_timeout_execute():
    timeout = 3  # TODO: the timeout should be smaller but as a workaround for Pypy slowness/overhead we set it to 3 sec
    func = execute.python(timeout=timeout)(sleep_and_return)

    # Normal case
    result = func(0.25)
    assert result == 0.25

    # Timeout case
    t = time.time()
    with pytest.raises(ExecutionTimeoutError) as e:
        func(10)
    assert (time.time() - t) < 10.0
    assert f"ExecutionTimeoutError after {timeout} seconds" in str(e.value)


def test_timeout_execute_from_thread():
    # From a thread
    t = threading.Thread(target=test_timeout_execute)
    t.start()
    t.join()


def create_sleeper_subprocess():
    pid = subprocess.Popen(["sleep", "600"]).pid
    return pid


@pytest.mark.xfail(
    platform.system() == "Darwin" or "PyPy" in sys.version,
    reason="psutil process statuses are buggy on OSX, and test randomly fails on PyPy",
)
def test_execute_dont_kill_children():
    pid = execute.python()(create_sleeper_subprocess)()
    subprocess = psutil.Process(pid)
    assert subprocess.status() == "sleeping"
    subprocess.terminate()  # cleanup


def test_execute_kill_children():
    pid = execute.python(kill_children=True)(create_sleeper_subprocess)()
    with pytest.raises(psutil.NoSuchProcess):
        psutil.Process(pid)


@execute.python()
def length(x):
    return len(x)


def test_large_command_line():
    x = "a" * 1024 * 1024
    assert length(x) == len(x)


def test_large_command_line_unicode():
    x = "ä" * 1024 * 1024
    assert length(x) == len(x)


def test_large_command_line_utf8():
    """
    UTF-8 bytes must be handled as Unicode, both in Python 2 and Python 3.
    """
    x = "ä" * 1024 * 1024
    assert length(x.encode("utf-8")) == len(x)
