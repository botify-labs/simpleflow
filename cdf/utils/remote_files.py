from cdf.utils.s3 import list_files


def nb_parts_from_crawl_location(s3_uri):
    """
    Return the number of parts from a raw data crawl location
    """
    files = list_files(s3_uri, regexp='urlids.txt.([0-9]+)')
    return len(files)
