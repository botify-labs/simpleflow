import re
from cdf.utils import s3, path


def nb_parts_from_crawl_location(uri):
    """
    Return the number of parts from a raw data crawl location
    """
    regexp = r'urlids\.txt\.([0-9]+)\.gz'
    if s3.is_s3_uri(uri):
        files = s3.list_files(uri, regexp=regexp)
    else:
        files = path.list_files(uri, regexp=regexp)
    return len(files)


def enumerate_partitions(uri):
    """
    Return the list of partition ids
    :param uri: the crawl data location (s3_uri)
    :type uri: str
    :returns: list - sorted list of urlids
    """
    regexp = r'urlids\.txt\.([0-9]+)\.gz'
    if s3.is_s3_uri(uri):
        files = s3.list_files(uri, regexp=regexp)
        files = [f.name for f in files]
    else:
        files = path.list_files(uri, regexp=regexp)
    pattern = r"urlids\.txt\.(\d+)\.gz$"
    pattern = re.compile(pattern)
    result = []
    for f in files:
        m = pattern.search(f)
        if m is None:
            continue
        result.append(int(m.group(1)))
    result = sorted(result)
    return result
