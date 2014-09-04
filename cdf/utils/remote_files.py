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
