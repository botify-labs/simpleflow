import os.path
import json
import itertools
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
from cdf.features.sitemap.document import SitemapDocument


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
    s3_download_status = DownloadStatus()
    for url in input_urls:
        crt_file_index = download_sitemap_file(url, s3_uri, tmp_dir, force_fetch)
        s3_download_status.update(crt_file_index)

    s3_subdir_uri = os.path.join(s3_uri, "sitemaps")

    #push the file that list the sitemap files
    s3.push_content(
        os.path.join(s3_subdir_uri, "download_status.json"),
        s3_download_status.to_json()
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
    :returns: DownloadStatus
    """
    download_status = download_sitemaps(input_url, tmp_dir)

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

    s3.fetch_file(
        os.path.join(s3_uri, 'sitemaps', 'download_status.json'),
        os.path.join(tmp_dir, 'download_status.json'),
        force_fetch
    )

    sitemap_only_filename = 'sitemap_only.gz'
    sitemap_only_filepath = os.path.join(tmp_dir,
                                         sitemap_only_filename)

    #for each url in the sitemaps
    dataset = SitemapStreamDef.create_temporary_dataset()
    with gzip.open(sitemap_only_filepath, 'wb') as sitemap_only_file:
        for url in get_sitemap_urls_stream(s3_uri, tmp_dir, force_fetch):
            urlid = url_to_id.get(url, None)
            if urlid is None:
                line = "{}\n".format(url)
                line = unicode(line)
                sitemap_only_file.write(line)
            else:
                dataset.append(urlid)
    dataset.persist_to_s3(s3_uri,
                          first_part_id_size=first_part_id_size,
                          part_id_size=part_id_size)
    s3.push_file(
        os.path.join(s3_uri, sitemap_only_filename),
        sitemap_only_filepath
    )


def get_sitemap_urls_stream(s3_uri, tmp_dir, force_fetch):
    """Return a stream made of the urls that are in the sitemaps
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    :param force_fetch: if True, the files will be downloaded from s3
                        even if they are in the tmp directory.
                        if False, files that are present in the tmp_directory
                        will not be downloaded from s3.
    :type force_fetch: bool
    :returns: iterator
    """
    download_status_filename = 'download_status.json'
    s3.fetch_file(
        os.path.join(s3_uri, 'sitemaps', 'download_status.json'),
        os.path.join(tmp_dir, 'download_status.json'),
        force_fetch
    )

    download_status = json.load(open(os.path.join(tmp_dir, "download_status.json")))
    sitemap_files = []
    for sitemap in download_status["sitemaps"]:
        sitemap_s3_uri = sitemap["s3_uri"]
        _, filename = s3.uri_parse(sitemap_s3_uri)
        destination = os.path.join(tmp_dir, filename)
        s3.fetch_file(
            os.path.join(sitemap_s3_uri),
            destination,
            force_fetch
        )
        sitemap_files.append(destination)

    sitemap_streams = []
    for sitemap_file in sitemap_files:
        sitemap_document = SitemapDocument(sitemap_file)
        sitemap_streams.append(sitemap_document.get_urls())
    return itertools.chain(*sitemap_streams)
