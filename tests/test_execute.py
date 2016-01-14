import tempfile
import os.path
import platform

import pytest

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


@pytest.mark.skipif(platform.system() == 'Darwin',
                    reason="ls doesn't have a --hide option on MacOSX")
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


@pytest.mark.skipif(platform.system() == 'Darwin',
                    reason="ls doesn't have a --hide option on MacOSX")
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


@execute.python()
class Add(object):
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
    with pytest.raises(TypeError):
        add('1')
    with pytest.raises(TypeError):
        Add('1')


@execute.python()
def print_string(s, retval):
    print s
    return retval


@execute.python()
class PrintString(object):
    def __init__(self, s, retval):
        self.s = s
        self.retval = retval

    def execute(self):
        print self.s
        return self.retval


def test_function_with_print():
    assert print_string("This isn't part of the return value", None) is None
    assert PrintString("This isn't part of the return value", None) is None


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
class RaiseDummyException(object):
    def __init__(self):
        pass

    @staticmethod
    def execute():
        raise DummyException


def test_function_as_program_raises_custom_exception():
    with pytest.raises(DummyException):
        raise_dummy_exception()
    with pytest.raises(DummyException):
        RaiseDummyException()


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


@execute.python()
def warn():
    import warnings
    warnings.warn("The _posixsubprocess module is not being used. "
                  "Child process reliability may suffer if your "
                  "program uses threads.", RuntimeWarning)
    raise StandardError('Fake Standard Error')


def test_function_with_warning():
    try:
        warn()
    except StandardError:
        pass
    else:
        assert False
