import urlparse
import os.path
import time
import json
from collections import namedtuple

from cdf.log import logger

from cdf.features.sitemap.exceptions import (UnhandledFileType,
                                             ParsingError,
                                             DownloadError)
from cdf.features.sitemap.utils import download_url
from cdf.features.sitemap.constant import DOWNLOAD_DELAY
from cdf.features.sitemap.document import (SiteMapType,
                                           SitemapDocument)

#FIXME add a source sitemap index if any(cf. https://github.com/sem-io/botify-cdf/issues/381)
Sitemap = namedtuple('Sitemap', ['url', 's3_uri', 'sitemap_index'])


class DownloadStatus(object):
    """A class information about the downloaded sitemaps:
        where they come from, where they are stored
        errors that occured
        :param sitemaps: the list of downloaded sitemaps.
                         each sitemap is an instance of Sitemap
        :type sitemaps: list
        :param errors: the list of sitemap errors. Each error is a string
                       representing an url.
        :type errors: list"""
    def __init__(self, sitemaps=None, errors=None):
        self.sitemaps = sitemaps or []
        self.errors = errors or []

    def add_success_sitemap(self, sitemap):
        """Add a sitemap that has been successfuly downloaded.
        :param sitemap: the input sitemap
        :type sitemap: Sitemap
        """
        self.sitemaps.append(sitemap)

    def add_error(self, url):
        """Add an error url
        :param url: the error url
        :type url: str
        """
        self.errors.append(url)

    def to_json(self):
        """Return a json representation of the object
        :returns: str"""
        d = {
            "sitemaps": [sitemap.__dict__ for sitemap in self.sitemaps],
            "errors": self.errors
        }
        return json.dumps(d)

    def __eq__(self, other):
        return (set(self.sitemaps) == set(other.sitemaps) and
                set(self.errors) == set(other.errors))

    def update(self, other):
        self.sitemaps.extend(other.sitemaps)
        self.errors.extend(other.errors)

def parse_download_status_from_json(file_path):
    """Build a DownloadStatus object from a json file
    :param file_path: the input file path
    :type file_path: str
    :returns: DownloadStatus
    """
    with open(file_path) as f:
        download_status = json.load(f)
    sitemaps = [Sitemap(sitemap["url"], sitemap["s3_uri"], sitemap.get("sitemap_index", None)) for sitemap
                in download_status["sitemaps"]]
    errors = download_status["errors"]
    result = DownloadStatus(sitemaps, errors)
    return result


def download_sitemaps(input_url, output_directory):
    """Download all sitemap files related to an input url in a directory.
    If the input url is a sitemap, the file will simply be downloaded,
    if it is a sitemap index, it will download the listed sitemaps.
    The function returns a dict original url -> path.
    If a file could not be downloaded, the path is None.
    :param input_url: the url to the sitemap or sitemap index file
    :type input_url: str
    :param output_directory: the path to the directory where to save the files
    :type output_directory: str
    :returns: DownloadStatus
    :raises: UnhandledFileType
    """
    result = DownloadStatus()
    #download input url
    output_file_path = get_output_file_path(input_url, output_directory)
    try:
        download_url(input_url, output_file_path)
    except DownloadError as e:
        logger.error("Download error: %s", e.message)
        result.add_error(input_url)
        return result

    sitemap_document = SitemapDocument(output_file_path)
    sitemap_type = sitemap_document.get_sitemap_type()
    #if it is a sitemap
    if sitemap_type == SiteMapType.SITEMAP:
        result.add_success_sitemap(Sitemap(input_url, output_file_path, None))
    #if it is a sitemap index
    elif sitemap_type == SiteMapType.SITEMAP_INDEX:
        #download referenced sitemaps
        try:
            result = download_sitemaps_from_urls(sitemap_document.get_urls(),
                                                 output_directory,
                                                 input_url)
        except ParsingError:
            result.add_error(input_url)
        #remove sitemap index file
        os.remove(output_file_path)
    else:
        raise UnhandledFileType("{} was not recognized as sitemap file.".format(input_url))
    return result


def download_sitemaps_from_urls(urls, output_directory, sitemap_index=None):
    """Download sitemap files from a list of urls.
    If the input url is a sitemap, the file will simply be downloaded.
    The function returns a dict url -> output file path
    If one can file could not be downloaded, the output file path is None.
    :param urls: a generator of input urls
    :type urls: generator
    :param output_directory: the path to the directory where to save the files
    :type output_directory: str
    :param sitemap_index: the url of the sitemap index
                          that lists all the input urls
    :type sitemap_index: str
    :returns: dict - a dict url -> output file path
    :raises: ParsingError - in case url generator raises
    """
    result = DownloadStatus()
    for url in urls:
        file_path = get_output_file_path(url, output_directory)
        time.sleep(DOWNLOAD_DELAY)
        try:
            download_url(url, file_path)
            sitemap_document = SitemapDocument(file_path)
            sitemap_type = sitemap_document.get_sitemap_type()
        except (DownloadError, ParsingError) as e:
            logger.error("Skipping {}: {}".format(url, e.message))
            if os.path.isfile(file_path):
                os.remove(file_path)
                result.add_error(url)
            continue
        #  check if it is actually a sitemap
        if sitemap_type == SiteMapType.SITEMAP:
            result.add_success_sitemap(Sitemap(url, file_path, sitemap_index))
        else:
            #  if not, remove file
            logger.warning("'%s' is not a sitemap file.", url)
            os.remove(file_path)
    return result


def get_output_file_path(url, output_directory):
    """Return the path where to save the content of an url.
    By default the function simply concatenates the output directory
    with the url basename.
    If the resulting path already exists, it appends a suffix "_2", "_3",
    until the resulting path does not exist.
    :param url: the input url
    :type url: str
    :param output_directory: the path to the directory
                             where to save the url content
    :type output_directory: str
    :returns: str
    """
    parsed_url = urlparse.urlparse(url)
    result = os.path.join(output_directory, os.path.basename(parsed_url.path))
    if not os.path.exists(result):
        return result
    #handle name collisions by appending a '_2','_3', etc. suffix
    index = 2
    while True:
        candidate_basename = "{}_{}".format(os.path.basename(parsed_url.path),
                                            index)
        candidate = os.path.join(output_directory, candidate_basename)
        if not os.path.exists(candidate):
            return candidate
        index += 1
