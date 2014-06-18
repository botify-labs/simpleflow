import os.path
import json

from cdf.utils import s3
from cdf.core.decorators import feature_enabled
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir

from cdf.features.sitemap.download import download_sitemaps


@with_temporary_dir
@feature_enabled('sitemap')
def download_sitemap_files(input_urls, s3_uri, tmp_dir=None, force_fetch=False):
    """Download all sitemap files related to a list of input url and upload them to s3.
    For each input url, If it is a sitemap, the file will simply be downloaded,
    if it is a sitemap index, it will download the listed sitemaps
    :param input_urls: a list of sitemap/sitemap index urls
    :type input_urls: list
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    """
    s3_file_index = {}
    for url in input_urls:
        crt_file_index = download_sitemap_file(url, s3_uri, tmp_dir, force_fetch)
        s3_file_index.update(crt_file_index)

    s3_subdir_uri = os.path.join(s3_uri, "sitemaps")

    #push the file that list the sitemap files
    s3.push_content(
        os.path.join(s3_subdir_uri, "download_status.json"),
        json.dumps(s3_file_index)
    )


def download_sitemap_file(input_url, s3_uri, tmp_dir=None, force_fetch=False):
    """Download all sitemap files related to an input url and upload them to s3.
    If the input url is a sitemap, the file will simply be downloaded,
    if it is a sitemap index, it will download the listed sitemaps
    The function returns a dict original url -> s3 uri.
    :param input_urls: a list of sitemap/sitemap index urls
    :type input_urls: list
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    :returns: dict
    """
    file_index = download_sitemaps(input_url, tmp_dir)

    s3_subdir_uri = os.path.join(s3_uri, "sitemaps")
    #a dict similar to file_locations but that stores s3 uris
    s3_file_index = {}
    for url, file_path in file_index.iteritems():
        destination_uri = os.path.join(s3_subdir_uri, os.path.basename(file_path))
        s3.push_file(
            os.path.join(destination_uri),
            file_path
        )
        s3_file_index[url] = destination_uri

    return s3_file_index
