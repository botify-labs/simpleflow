import re
import os
from cdf.exceptions import MalformedFileNameError
from cdf.utils import s3, path
from cdf.utils.crawl_info import get_crawl_info, get_max_crawled_partid
from cdf.core import constants


def enumerate_partitions(uri, only_crawled_urls=False):
    """
    Return the list of partition ids
    :param uri: the crawl data location (s3_uri)
    :type uri: str
    :param only_crawled_urls : Return partitions with at less 1 url crawled inside
    :type only_crawled_urls : boolean
    :returns: list - sorted list of urlids
    """
    regexp = r'urlids\.txt\.([0-9]+)\.gz'
    if s3.is_s3_uri(uri):
        files = s3.list_files(uri, regexp=regexp)
        files = [f.name for f in files]
    else:
        files = path.list_files(uri, regexp=regexp)
    result = [get_part_id_from_filename(f) for f in files]
    result = sorted(result)

    if only_crawled_urls:
        crawl_info = get_crawl_info(uri)
        max_part_id = get_max_crawled_partid(crawl_info)
        return filter(lambda p: p <= max_part_id, result)

    return result


def get_part_id_from_filename(filename):
    """Return the part id from a filename
    If the part id can not be extracted raise a MalformedFileNameError

    :param filename: the input filename
    :type filename: str
    :returns: int -- the part id
    """
    regex = re.compile(".*txt.([\d]+).gz")
    m = regex.match(filename)
    if not m:
        raise MalformedFileNameError(
            "%s does not contained any part id." % filename
        )
    return int(m.group(1))
