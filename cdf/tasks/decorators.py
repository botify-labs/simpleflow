import tempfile
import functools
import shutil
import contextlib

from cdf.utils.path import makedirs


TMP_KEY = 'tmp_dir'
CLEAN_KEY = 'cleanup'


@contextlib.contextmanager
def temporary_directory(tmp_dir=None, remove_on_exit=True):
    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp('_task')
    else:
        makedirs(tmp_dir, exist_ok=True)
    try:
        yield tmp_dir
    finally:
        if remove_on_exit:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def with_temporary_dir(func):
    """Create a temporary directory for a task, execute it, then clean up

    This class aims to be called as decorator on functions needing to
    fetch external files that will be removed at the end of the task.

    The wrapped function must have a `tmp_dir` named argument.
        my_func(some_param, tmp_dir=None)
    The temporary dir is injected using kwargs.

    There are 2 possible behaviors of the wrapped function:
        - `tmp_dir` is missing or `None`:

          The function is executed in a temporary directory with cleanup

        - `tmp_dir` is set and `cleanup` is True (defaults to False).

          The function is executed in the given directory. The cleanup
          depends on the `cleanup` kwargs.

    It means if a directory is set, user can cancel the cleanup (for tests).
    However when using a auto generated directory, the cleanup is always
    performed.

    Ex :
        >>> @with_temporary_dir
        >>> def my_task(crawl_id, tmp_dir=None, force_fetch=True):
        >>>     pass

    The call `my_task(1000)` will be executed in a temp directory which
    is removed after the task execution.
    """
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        # execute in a temp directory ?
        tmp_dir = kwargs.get(TMP_KEY)
        is_tmp = tmp_dir is None

        # pops the `cleanup` kwargs
        should_clean = kwargs.pop(CLEAN_KEY, False)
        # if execute in a temp directory, always cleanup
        should_clean = is_tmp or should_clean

        with temporary_directory(tmp_dir, should_clean) as tmp:
            kwargs[TMP_KEY] = tmp
            result = func(*args, **kwargs)

        return result

    return wrap


# back-compatibility
# in this way old imports don't get broken
TemporaryDirTask = with_temporary_dir