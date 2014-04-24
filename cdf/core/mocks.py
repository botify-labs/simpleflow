import shutil
import re
import os
from boto.exception import S3ResponseError


# Mock s3 module
def _list_local_files(input_dir, regexp):
    """List all files that satisfy the given regexp"""
    result = []

    def listdir(input_dir):
        """Recursively list all files under `input_dir`"""
        files = []
        for dir, _, filenames in os.walk(input_dir):
            files.extend([os.path.join(dir, f) for f in filenames])
        return files

    for f in listdir(input_dir):
        # exclude the top folder for regexp matching
        to_match = f[len(input_dir) + 1:]
        # regexp is a string, try match it
        if isinstance(regexp, str) and re.match(regexp, to_match):
            result.append(f)
        # regexp is a list of string, try match anyone of it
        elif isinstance(regexp, (list, tuple)):
            if any(re.match(r, to_match) for r in regexp):
                result.append(f)

    return result


def _mock_push_file(s3_uri, filename):
    """
    :param s3_uri : a local path prefixed by s3://
    """
    shutil.copy(filename, s3_uri[5:])


def _mock_push_content(s3_uri, content):
    """No push to s3"""
    pass


def _mock_fetch_file(s3_uri, dest_path, force_fetch, lock=True):
    """
    Fetch file from a fake S3 Uri (s3://local_path)
    """
    if not os.path.exists(s3_uri[5:]):
        # this should be managed correctly by tasks
        raise S3ResponseError('', '{} not found'.format(dest_path))
    else:
        shutil.copy(s3_uri[5:], dest_path)
        return dest_path, True


def _mock_fetch_files(s3_uri, dest_dir,
                      regexp=None, force_fetch=True, lock=True):
    """
    :param s3_uri : a local path prefixed by s3://
    """
    local_files = _list_local_files(s3_uri[5:], regexp)

    files = []
    for f in local_files:
        new_file = os.path.join(dest_dir, os.path.basename(f))
        shutil.copy(f, os.path.join(dest_dir, os.path.basename(f)))
        files.append((new_file, True))

    return files
