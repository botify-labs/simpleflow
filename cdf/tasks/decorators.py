import tempfile
import shutil
from cdf.utils.path import makedirs


TMP_KEY = 'tmp_dir'
CLEAN_KEY = 'cleanup'


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
    def wrap(*args, **kwargs):
        # execute in a temp directory ?
        is_tmp = TMP_KEY not in kwargs or kwargs[TMP_KEY] is None

        # pops the `cleanup` kwargs
        is_clean = kwargs.pop(CLEAN_KEY, False)
        # if execute in a temp directory, always cleanup
        is_clean = is_tmp or is_clean

        tmp_dir = None
        try:
            if is_tmp:
                # execute in temp dir
                tmp_dir = tempfile.mkdtemp('_task')
                kwargs[TMP_KEY] = tmp_dir
            else:
                # set the dirpath for cleanup
                tmp_dir = kwargs[TMP_KEY]
                makedirs(tmp_dir, exist_ok=True)

            # execution
            result = func(*args, **kwargs)

            return result

        except Exception:
            raise

        finally:
            if is_clean:
                shutil.rmtree(tmp_dir, ignore_errors=True)

    return wrap


# back-compatibility
# in this way old imports don't get broken
TemporaryDirTask = with_temporary_dir