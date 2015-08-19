import tempfile
import os.path

import pytest

from simpleflow import activity
from simpleflow import execute


@execute.program(path='ls')
def ls_nokwargs(*args):
    """
    Only accepts a variable number of positional arguments.

    """
    pass


def test_execute_program_no_kwargs():
    with tempfile.NamedTemporaryFile() as f:
        with pytest.raises(TypeError) as exc_info:
            ls_nokwargs(hide=f.name)

        assert (exc_info.value.message ==
                'command does not take keyword arguments')


@execute.program(path='ls')
def ls_noargs(**kwargs):
    """
    Only accepts a variable number of keyword arguments.

    """
    pass


def test_execute_program_no_args():
    with tempfile.NamedTemporaryFile() as f:
        with pytest.raises(TypeError) as exc_info:
            ls_noargs(f.name)

        assert (exc_info.value.message ==
                'command does not take varargs')


@execute.program(path='ls')
def ls_restrict_named_arguments(hide=execute.RequiredArgument, *args):
    pass


def test_execute_program_restrict_named_arguments():
    with tempfile.NamedTemporaryFile() as f:
        with pytest.raises(TypeError) as exc_info:
            ls_restrict_named_arguments(f.name)

        assert (exc_info.value.message ==
                'argument "hide" not found')


@execute.program(path='ls')
def ls_optional_named_arguments(hide='', *args):
    pass


def test_execute_program_optional_named_arguments():
    with tempfile.NamedTemporaryFile() as f:
        assert ls_optional_named_arguments(f.name).strip() == f.name
        assert f.name not in ls_optional_named_arguments(hide=f.name)


@execute.program()
def ls(*args, **kwargs):
    pass


def test_execute_program_with_positional_arguments():
    with tempfile.NamedTemporaryFile() as f:
        assert ls(f.name).strip() == f.name


def test_execute_program_with_named_arguments():
    with tempfile.NamedTemporaryFile() as f:
        assert f.name not in (ls(
            os.path.dirname(f.name),
            hide=f.name).strip())


@execute.program()
def ls_2args(a, b):
    pass


def test_ls_2args():
    with pytest.raises(TypeError) as exc_info:
        ls_2args(1, 2, 3)

    assert (exc_info.value.message ==
            'command takes 2 arguments: 3 passed')


@execute.python()
def inc(xs):
    return [x + 1 for x in xs]


def test_function_as_program():
    assert inc([1, 2, 3]) == [2, 3, 4]


@execute.python()
def add(a, b=1):
    return a + b


def test_function_as_program_with_default_kwarg():
    assert add(4) == 5


def test_function_as_program_with_kwargs():
    assert add(3, 7) == 10


def test_function_as_program_raises_builtin_exception():
    with pytest.raises(TypeError):
        add('1')


@execute.python()
def print_string():
    print "This isn't part of the return value"


def test_function_with_print():
    assert print_string() is None


class DummyException(Exception):
    pass


@execute.python()
def raise_dummy_exception():
    raise DummyException


def test_function_as_program_raises_custom_exception():
    with pytest.raises(DummyException):
        raise_dummy_exception()


@execute.python()
def raise_timeout_error():
    from simpleflow.exceptions import TimeoutError
    raise TimeoutError('timeout', 1)


def test_function_as_program_raises_module_exception():
    from simpleflow.exceptions import TimeoutError

    err = None
    try:
        raise_timeout_error()
    except TimeoutError as err:
        assert err.timeout_type == 'timeout'
        assert err.timeout_value == 1
    else:
        assert False

    assert isinstance(err, TimeoutError)
