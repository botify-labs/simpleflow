import gzip
import os
import errno
import itertools
import codecs
import re

from cdf.analysis.urls.utils import get_part_id
from cdf.log import logger


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
        logger.info('Writing for file {}'.format(file_name))
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


def get_files_ordered_by_part_id(directory, file_identifier):
    """
    Return a list of files ordered by part_id
    """
    file_by_part_id = {}
    pattern = "{}.txt.([0-9]+).gz".format(file_identifier)
    file_regexp = re.compile(pattern)
    for f in sorted(os.listdir(directory)):
        m = file_regexp.match(f)
        if m:
            file_by_part_id[int(m.groups()[0])] = f
    return [file_by_part_id[k] for k in sorted(file_by_part_id)]
