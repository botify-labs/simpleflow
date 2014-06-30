import os
import itertools
import re
from urlparse import urlparse

from cdf.utils import s3
from cdf.features.sitemap.document import SitemapDocument
from cdf.features.sitemap.download import parse_download_status_from_json


def match_sitemap_urls_from_stream(url_generator,
                                   url_to_id,
                                   dataset,
                                   domain_validator,
                                   sitemap_only_file,
                                   out_of_crawl_domain):
    """The method matches sitemap urls from a stream
    to the urls in the sitemap.
    If the url is in the crawl, we add its url id in an output stream.
    If not we write the url itself in an output file.
    :param url_generator: an iterator over the urls in the sitemap
    :type url_generator: iterator
    :param url_to_id: a dict for the urls in the crawl url->urlid
    :type url_to_id: dict
    :param dataset: the dataset where to store urlids for urls that are both in
                    sitemap and in crawl
    :type dataset: TemporaryDataset
    :param sitemap_only_file: a file object where to store urls that are only
                              in the sitemap and in the crawl domain
    :param out_of_crawl_domain: a file object where to store urls that are only
                              in the sitemap and not in the crawl domain
    """
    for url in url_generator:
        urlid = url_to_id.get(url, None)
        if urlid is None:
            line = "{}\n".format(url)
            line = unicode(line)
            if domain_validator.is_valid(url):
                sitemap_only_file.write(line)
            else:
                out_of_crawl_domain.write(line)
        else:
            dataset.append(urlid)


def get_download_status_from_s3(s3_uri, tmp_dir, force_fetch):
    """Get the sitemap download status corresponding to a crawl.
    The function downloads the corresponding file and builds a DownloadStatus
    object from it.
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    :param force_fetch: if True, the files will be downloaded from s3
                        even if they are in the tmp directory.
                        if False, files that are present in the tmp_directory
                        will not be downloaded from s3.
    :type force_fetch: bool
    :returns: DownloadStatus
    """
    download_status_filename = 'download_status.json'
    s3.fetch_file(
        os.path.join(s3_uri, 'sitemaps', download_status_filename),
        os.path.join(tmp_dir, 'download_status.json'),
        force_fetch
    )

    result = parse_download_status_from_json(
        os.path.join(tmp_dir, download_status_filename)
    )
    return result


def download_sitemaps_from_s3(s3_uri, tmp_dir, force_fetch):
    """Download the sitemap files stored on s3.
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    :param force_fetch: if True, the files will be downloaded from s3
                        even if they are in the tmp directory.
                        if False, files that are present in the tmp_directory
                        will not be downloaded from s3.
    :type force_fetch: bool
    """
    download_status = get_download_status_from_s3(s3_uri, tmp_dir, force_fetch)
    sitemap_files = []
    for sitemap in download_status.sitemaps:
        _, filename = s3.uri_parse(sitemap.s3_uri)
        destination = os.path.join(tmp_dir, filename)
        s3.fetch_file(
            os.path.join(sitemap.s3_uri),
            destination,
            force_fetch
        )
        sitemap_files.append(destination)
    return sitemap_files


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
    sitemap_files = download_sitemaps_from_s3(s3_uri, tmp_dir, force_fetch)
    sitemap_streams = []
    for sitemap_file in sitemap_files:
        sitemap_document = SitemapDocument(sitemap_file)
        sitemap_streams.append(sitemap_document.get_urls())
    return itertools.chain(*sitemap_streams)


class DomainValidator(object):
    """A class to check if a domain should be crawled or not
    The decision is made on the url domain.
    The user specifies a list of allowed domains.
    Allowed domains can contain a '*' wildcard
    that can be replaced by anything.
    The user can also specifies a list of blacklisted domains.
    Black listed domains can not contain any wildcard."""
    def __init__(self, allowed_domains, blacklisted_domains=None):
        """Constructor
        :param allowed_domains: the list of allowed domains
        :type allowed_domains: list
        :param blacklisted_domains: the list of black listed domains
        :type blacklisted_domains: list
        """
        self.allowed_domains = allowed_domains
        self.allowed_patterns = [self.get_compiled_pattern(domain) for
                                 domain in allowed_domains]
        self.blacklisted_domains = blacklisted_domains or []
        self.blacklisted_domains = frozenset(self.blacklisted_domains)

    def get_compiled_pattern(self, allowed_domain):
        """Return a compiled regex corresponding to one allowed_domain.
        :param allowed_domain: the input domain
        :type allowed_domain: str
        :returns: _sre.SRE_Pattern
        """
        regex_pattern = re.escape(allowed_domain)
        regex_pattern = regex_pattern.replace("\\*", ".*")
        return re.compile(regex_pattern)

    def is_valid(self, url):
        """Decide whether or not an url is valid
        given the allowed domains
        :param url: the input url
        :type url: str
        :returns: bool"""
        domain = urlparse(url).netloc
        #TODO depending on regex performance,
        #we could create a set of allowed domains for domains without wildcard
        #and use regex only for domains with wildcard
        result = any(itertools.imap(lambda p: p.match(domain), self.allowed_patterns))
        result &= not domain in self.blacklisted_domains
        return result
