import os.path
import gzip

from cdf.utils import s3
from cdf.core.decorators import feature_enabled
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir
from cdf.features.main.utils import get_url_to_id_dict_from_stream

from cdf.features.main.streams import IdStreamDef
from cdf.core.constants import FIRST_PART_ID_SIZE, PART_ID_SIZE
from cdf.features.sitemap.download import (download_sitemaps,
                                           Sitemap,
                                           DownloadStatus)
from cdf.features.sitemap.streams import SitemapStreamDef
from cdf.features.sitemap.matching import (match_sitemap_urls_from_stream,
                                           get_sitemap_urls_stream)


@with_temporary_dir
@feature_enabled('sitemap')
def download_sitemap_files(input_urls,
                           s3_uri,
                           user_agent=None,
                           tmp_dir=None,
                           force_fetch=False):
    """Download all sitemap files related to a list of input url and upload them to s3.
    For each input url, If it is a sitemap, the file will simply be downloaded,
    if it is a sitemap index, it will download the listed sitemaps
    :param input_urls: a list of sitemap/sitemap index urls
    :type input_urls: list
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param user_agent: the user agent to use for the query.
                       If None, uses 'requests' default user agent
    :type user_agent: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    """
    s3_download_status = DownloadStatus()
    for url in input_urls:
        crt_file_index = download_sitemap_file(url,
                                               s3_uri,
                                               user_agent,
                                               tmp_dir,
                                               force_fetch)
        s3_download_status.update(crt_file_index)

    s3_subdir_uri = os.path.join(s3_uri, "sitemaps")

    #push the file that list the sitemap files
    s3.push_content(
        os.path.join(s3_subdir_uri, "download_status.json"),
        s3_download_status.to_json()
    )


def download_sitemap_file(input_url,
                          s3_uri,
                          user_agent=None,
                          tmp_dir=None,
                          force_fetch=False):
    """Download all sitemap files related to an input url and upload them to s3.
    If the input url is a sitemap, the file will simply be downloaded,
    if it is a sitemap index, it will download the listed sitemaps
    The function returns a dict original url -> s3 uri.
    :param input_url: a sitemap/sitemap index url
    :type input_url: str
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param user_agent: the user agent to use for the query.
                       If None, uses 'requests' default user agent
    :type user_agent: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    :returns: DownloadStatus
    """
    download_status = download_sitemaps(input_url, tmp_dir, user_agent)

    s3_subdir_uri = os.path.join(s3_uri, "sitemaps")
    #an object similar to download_status but that stores s3 uris
    s3_download_status = DownloadStatus(errors=download_status.errors)
    for sitemap in download_status.sitemaps:
        url, file_path, sitemap_index = sitemap
        destination_uri = os.path.join(s3_subdir_uri, os.path.basename(file_path))
        s3.push_file(
            os.path.join(destination_uri),
            file_path
        )
        s3_download_status.add_success_sitemap(Sitemap(url, destination_uri, sitemap_index))

    return s3_download_status


@with_temporary_dir
@feature_enabled('sitemap')
def match_sitemap_urls(s3_uri,
                       first_part_id_size=FIRST_PART_ID_SIZE,
                       part_id_size=PART_ID_SIZE,
                       tmp_dir=None,
                       force_fetch=False):
    """Match urls from the sitemaps to urls from the crawl.
    When the url is present in the crawl, we save its urlid in a file
    'sitemap.txt.XXX.gz', when it is not present, the full url is
    saved in a file 'sitemap_only.gz'.
    The generated files are then pushed to s3.
    """

    #load crawl information
    id_stream = IdStreamDef.get_stream_from_s3(s3_uri, tmp_dir=tmp_dir)
    url_to_id = get_url_to_id_dict_from_stream(id_stream)
    #download sitemaps

    sitemap_only_filename = 'sitemap_only.gz'
    sitemap_only_filepath = os.path.join(tmp_dir,
                                         sitemap_only_filename)

    dataset = SitemapStreamDef.create_temporary_dataset()
    with gzip.open(sitemap_only_filepath, 'wb') as sitemap_only_file:
        url_generator = get_sitemap_urls_stream(s3_uri, tmp_dir, force_fetch)
        match_sitemap_urls_from_stream(
            url_generator,
            url_to_id,
            dataset,
            sitemap_only_file)
    dataset.persist_to_s3(s3_uri,
                          first_part_id_size=first_part_id_size,
                          part_id_size=part_id_size)
    s3.push_file(
        os.path.join(s3_uri, sitemap_only_filename),
        sitemap_only_filepath
    )
