import errno
import functools
import hashlib
import os

from lockfile import FileLock

from simpleflow import logger
from simpleflow.settings import SIMPLEFLOW_BINARIES_DIRECTORY
from simpleflow.storage import pull


class RemoteBinary(object):
    """
    This class identifies if binary is available in $PATH so it can be used.
    If the binary is not available, simpleflow will try to download it and
    put it in a dedicated folder so it can be used. It will also prepend this
    folder to $PATH before forking to the real activity worker.
    """
    def __init__(self, name, remote_location):
        """
        :param name: name of the binary to be downloaded
        :type  name: str
        :param remote_location: remote location where to download the binary from (only S3 for now)
        :type  remote_location: str
        """
        self.name = name

        # limit ourselves to S3 for now
        assert remote_location.startswith("s3://")
        self.remote_location = remote_location
        self.local_directory = self._compute_local_directory()
        self.local_location = self._compute_local_location()
        self.lock_location = self._compute_lock_location()

    def download(self):
        self._mkdir_p(self.local_directory)
        with FileLock(self.lock_location):
            if not self._check_binary_present():
                self._download_binary()

    def _mkdir_p(self, path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def _compute_local_directory(self):
        suffix = hashlib.md5(self.remote_location.encode("utf-8")).hexdigest()
        return os.path.join(SIMPLEFLOW_BINARIES_DIRECTORY, "{}-{}".format(self.name, suffix))

    def _compute_local_location(self):
        return os.path.join(self.local_directory, self.name)

    def _compute_lock_location(self):
        return os.path.join(self.local_directory, ".{}.lock".format(self.name))

    def _check_binary_present(self):
        return os.access(self.local_location, os.X_OK)

    def _download_binary(self):
        logger.info("Downloading binary: {} -> {}".format(self.remote_location, self.local_location))
        bucket, path = self.remote_location.replace("s3://", "", 1).split("/", 1)
        # with FileLock(dest):
        pull(bucket, path, self.local_location)
        os.chmod(self.local_location, 0o755)


# convenience helpers
def download_binaries(binaries_map):
    for binary, remote_location in binaries_map.items():
        binary = RemoteBinary(binary, remote_location)
        binary.download()
        os.environ["PATH"] = binary.local_directory + ":" + os.environ["PATH"]


def with_binaries(binaries_map):
    def decorator(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            download_binaries(binaries_map)
            return func(*args, **kwargs)
        wrapped.__wrapped__ = func
        return wrapped
    return decorator
