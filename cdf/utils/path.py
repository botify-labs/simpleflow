import gzip
import os
import errno
import itertools
import codecs
import re

from cdf.analysis.urls.utils import get_part_id
from cdf.log import logger


def list_files(dirpath, full_path=True, regexp=None):
    """List files in a local directory

    :param dirpath: path to the directory
    :type dirpath: str
    :param full_path: should the listed files contains the full path
    :type full_path: bool
    :param regexp: regexp for filtering the listed files
    :type regexp: list | tuple | str
    :return: file_list
    :rtype: list
    """
    files = []
    for f in os.listdir(dirpath):
        basename = os.path.basename(f)
        if (not regexp
            or (isinstance(regexp, str) and re.match(regexp, basename))
            or (isinstance(regexp, (list, tuple))
                and any(re.match(r, basename) for r in regexp))):
            if full_path:
                files.append(os.path.join(dirpath, f))
            else:
                files.append(f)

    return files


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


def group_by_part(generator, first_part_size, part_size):
    """Get row groups from a generator function,
    according to partition size params"""

    def get_url_part(url_id, *args):
        return get_part_id(url_id, first_part_size, part_size)

    return itertools.groupby(generator,
                             lambda fields: get_url_part(*fields))


def write_by_part(generator,
                  first_part_size, part_size,
                  dirpath, file_pattern, to_string_func):
    """Write rows from the generator function in partitions,
    according to the partition size params

    :param to_string_func: Determine the logic to transform a generator output to a string.
                          It should append '\n'
    """
    for part, rows in group_by_part(generator, first_part_size, part_size):
        file_name = file_pattern.format(part)
        logger.info('Writing for file %s' % file_name)
        path = os.path.join(dirpath, file_name)
        with gzip.open(path, 'w') as f:
            for row in rows:
                f.write(to_string_func(row))


def utf8_writer(file_handler):
    """Returns a wrapped file writer for utf-8 content

    :param file_handler: a file handler to be wrapped
    """
    return codecs.getwriter("utf-8")(file_handler)


def utf8_reader(file_handler):
    """Returns a wrapped file reader for utf-8 content

    :param file_handler: a file handler to be wrapped
    """
    return codecs.getreader("utf-8")(file_handler)


def partition_aware_sort(file_list, basename_func=os.path.basename):
    """Sort the file list in a partition aware fashion

    Assume that partition number locates at the last but one:
        filename.type.{partition_number}.file_extension

    User can also provide a basename extraction function to work with
    list of file objects, eg. Amazon S3 key objects

    :param file_list: list of files (or other file representing objects)
        to sort
    :param basename_func: function to extract the file basename out of
        the `file_list` item
    :return: sorted file_list
    """
    partition_regexp = re.compile(r'.*\.([0-9]+)\.[^\.]+')
    # if not all files are Botify's partitioned file, use lexical sort
    if not all(map(lambda i: partition_regexp.match(basename_func(i)),
                   file_list)):
        logger.warn("Apply partition-aware sort on non-partitioned files, "
                    "fall back to lexical sort")
        return sorted(file_list, key=basename_func)

    def key_func(f):
        basename = basename_func(f)
        return int(partition_regexp.findall(basename)[0])

    return sorted(file_list, key=key_func)
