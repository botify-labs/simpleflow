import tempfile
import os.path

import pytest

from simpleflow import activity


@activity.execute_program(path='ls')
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


@activity.execute_program(path='ls')
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


@activity.execute_program(path='ls')
def ls_restrict_named_arguments(hide=activity.RequiredArgument, *args):
    pass


def test_execute_program_restrict_named_arguments():
    with tempfile.NamedTemporaryFile() as f:
        with pytest.raises(TypeError) as exc_info:
            ls_restrict_named_arguments(f.name)

        assert (exc_info.value.message ==
                'argument "hide" not found')


@activity.execute_program(path='ls')
def ls_optional_named_arguments(hide='', *args):
    pass


def test_execute_program_optional_named_arguments():
    with tempfile.NamedTemporaryFile() as f:
        assert ls_optional_named_arguments(f.name).strip() == f.name
        assert f.name not in ls_optional_named_arguments(hide=f.name)


@activity.execute_program()
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


@activity.execute_program()
def ls_2args(a, b):
    pass


def test_ls_2args():
    with pytest.raises(TypeError) as exc_info:
        ls_2args(1, 2, 3)

    assert (exc_info.value.message ==
            'command takes 2 arguments: 3 passed')
