import os
import errno


def makedirs(directory_path, exist_ok=False):
    """Create a directory recursively.
    directory_path : the path to the directory to create
    exists_ok : if True, the method does not raise
                if directory_path already exists.
                This parameter mimics the os.makedirs()
                parameter that exists in python >= 3.2
    """
    try:
        os.makedirs(directory_path)
    except OSError as exc:
        if (exist_ok
            and exc.errno == errno.EEXIST
            and os.path.isdir(directory_path)):
                #don't raise if the directory already exists.
                pass
        else:
                raise
