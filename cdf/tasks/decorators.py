import tempfile
import shutil
from cdf.utils.path import makedirs


class TemporaryDirTask(object):
    """Create a temporary directory for a task, execute it, then clean up

    This class aims to be called as decorator on functions needing to
    fetch external files that will be removed at the end of the task.

    The wrapped function must have a `tmp_dir` named argument.
        my_func(some_param, tmp_dir=None)
    The temporary dir is injected using kwargs.

    There are 2 possible behaviors of the wrapped function:
        - `tmp_dir` is missing or `None`:

          The function is executed in a temporary directory with cleanup

        - `tmp_dir` is set and `cleanup` is True (default value).

          The function is executed in the given directory. The cleanup
          depends on the `cleanup` kwargs.

    It means if a directory is set, user can cancel the cleanup (for tests).
    However when using a auto generated directory, the cleanup is always
    performed.

    Ex :
        >>> @TemporaryDirTask
        >>> def myfunc(crawl_id, tmp_dir=None, force_fetch=True):
        >>>     pass

    The call `myfunc(1000)` will be executed in a temp directory and it
    is removed after execution.
    """
    tmp_key = 'tmp_dir'
    clean_key = 'cleanup'

    def __init__(self, func):
        self.func = func
        self.tmp_dir = None

    def setup(self):
        self.tmp_dir = tempfile.mkdtemp()

    def cleanup(self):
        shutil.rmtree(self.tmp_dir)

    def __call__(self, *args, **kwargs):
        # execute in a temp directory ?
        is_tmp = self.tmp_key not in kwargs or kwargs[self.tmp_key] is None

        # pops the `cleanup` kwargs
        is_clean = kwargs.pop(self.clean_key, False)
        # if execute in a temp directory, always cleanup
        is_clean = is_tmp or is_clean

        try:
            if is_tmp:
                # execute in temp dir
                self.setup()
                kwargs[self.tmp_key] = self.tmp_dir
                result = self.func(*args, **kwargs)
            else:
                # set the dirpath for cleanup
                self.tmp_dir = kwargs[self.tmp_key]
                makedirs(self.tmp_dir, exist_ok=True)
                # execute in the given dir
                result = self.func(*args, **kwargs)

            if is_clean:
                self.cleanup()

            return result

        except Exception:
            self.cleanup()
            raise