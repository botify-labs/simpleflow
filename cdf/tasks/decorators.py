import tempfile
import shutil


class TemporaryDirTask(object):
    """
    This class aims to be called as decorator on functions needing to fetch S3 Files that will be removed at the end of the task

    Functions meed to have a `tmp_dir` argument

    Ex :
    >>> @TemporaryDirTask
    >>> def myfunc(crawl_id, tmp_dir, force_fetch):
    >>>     pass

    Calling `myfunc.run(*args, **kwargs),
    will remove `tmp_dir` from the system at the end of the process

    Calling directly `myfunc(*args, **kwargs)` have no effect on `tmp_dir`
    """

    def __init__(self, func):
        self.func = func

    def setup(self):
        self.tmp_dir = tempfile.mkdtemp()

    def cleanup(self):
        shutil.rmtree(self.tmp_dir)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def run_in_temporary_dir(self, *args, **kwargs):
        self.setup()
        try:
            result = self.func(*args, tmp_dir=self.tmp_dir, **kwargs)
        except Exception, e:
            self.cleanup()
            raise e
        self.cleanup()
        return result
