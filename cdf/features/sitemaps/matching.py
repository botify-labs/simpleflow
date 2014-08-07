import os
import itertools
import re
from urlparse import urlparse

from cdf.utils import s3
from cdf.features.sitemaps.exceptions import ParsingError
from cdf.features.sitemaps.document import instanciate_sitemap_document
from cdf.features.sitemaps.metadata import parse_download_status_from_json


def match_sitemap_urls_from_documents(documents,
                                      url_to_id,
                                      dataset,
                                      domain_validator,
                                      nb_samples_to_keep,
                                      sitemap_only_urls,
                                      out_of_crawl_domain_urls):
    """The method matches sitemap urls from a list of documents
    to the urls in the sitemap.
    If the url is in the crawl, we add its url id in an output stream.
    If not we write the url itself in an output file.
    :param sitemap_documents: the list of input sitemap documents
    :type sitemap_documents: list
    :param url_to_id: a dict for the urls in the crawl url->urlid
    :type url_to_id: dict
    :param dataset: the dataset where to store urlids for urls that are both in
                    sitemap and in crawl
    :type dataset: TemporaryDataset
    :param nb_samples_to_keep: the maximum number of distinct urls to keep
                               for urls that are only in the sitemaps.
    :type nb_samples_to_keep: int
    :param sitemap_only_url: a list where to add samples urls that are
                             in the sitemaps but not in the crawl.
    :type sitemap_only_url: list
    :param out_of_crawl_domain_urls: a list where to add samples urls that are
                                     in the sitemaps but out of crawl domain.
    :type out_of_crawl_domain_urls: list
    """
    for document in documents:
        try:
            match_sitemap_urls_from_document(document,
                                             url_to_id,
                                             dataset,
                                             domain_validator,
                                             nb_samples_to_keep,
                                             sitemap_only_urls,
                                             out_of_crawl_domain_urls)
        except ParsingError as e:
            document.set_error(e.__class__.__name__, e.message)


def match_sitemap_urls_from_document(document,
                                     url_to_id,
                                     dataset,
                                     domain_validator,
                                     nb_samples_to_keep,
                                     sitemap_only_urls,
                                     out_of_crawl_domain_urls):
    """The method matches sitemap urls from a document
    to the urls in the sitemap.
    If the url is in the crawl, we add its url id in an output stream.
    If not we write the url itself in an output file.
    :param document: the input sitemap document
    :type document: SitemapDocument
    :param url_to_id: a dict for the urls in the crawl url->urlid
    :type url_to_id: dict
    :param dataset: the dataset where to store urlids for urls that are both in
                    sitemap and in crawl
    :type dataset: TemporaryDataset
    :param nb_samples_to_keep: the maximum number of distinct urls to keep
                               for urls that are only in the sitemaps.
    :type nb_samples_to_keep: int
    :param sitemap_only_url: a list where to add samples urls that are
                             in the sitemaps but not in the crawl.
    :type sitemap_only_url: list
    :param out_of_crawl_domain_urls: a list where to add samples urls that are
                                     in the sitemaps but out of crawl domain.
    :type out_of_crawl_domain_urls: list
    :raises: ParsingError
    """
    #build a set to be able to test quickly if a sitemap only url
    #has already been seen
    sitemap_only_urls_set = set(sitemap_only_urls)
    out_of_crawl_domain_urls_set = set(out_of_crawl_domain_urls)
    for url in document.get_urls():
        urlid = url_to_id.get(url, None)
        if urlid is None:
            if domain_validator.is_valid(url):
                list_to_update = sitemap_only_urls
                set_to_update = sitemap_only_urls_set
            else:
                list_to_update = out_of_crawl_domain_urls
                set_to_update = out_of_crawl_domain_urls_set

            if (len(list_to_update) < nb_samples_to_keep and url not in set_to_update):
                list_to_update.append(url)
                set_to_update.add(url)
        else:
            dataset.append(urlid)


def get_download_metadata_from_s3(s3_uri, tmp_dir, force_fetch):
    """Get the sitemap download metadata corresponding to a crawl.
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
    download_metadata_filename = 'sitemap_download_metadata.json'
    s3.fetch_file(
        os.path.join(s3_uri, 'sitemaps', download_metadata_filename),
        os.path.join(tmp_dir, 'sitemap_download_metadata.json'),
        force_fetch
    )

    result = parse_download_status_from_json(
        os.path.join(tmp_dir, download_metadata_filename)
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
    :returns: list - a list of tuples (file_path, original url)
    """
    download_metadata = get_download_metadata_from_s3(s3_uri, tmp_dir, force_fetch)
    sitemap_files = []
    for sitemap in download_metadata.sitemaps:
        _, filename = s3.uri_parse(sitemap.s3_uri)
        destination = os.path.join(tmp_dir, filename)
        s3.fetch_file(
            os.path.join(sitemap.s3_uri),
            destination,
            force_fetch
        )
        sitemap_files.append((destination, sitemap.url))
    return sitemap_files


def get_sitemap_documents(s3_uri, tmp_dir, force_fetch):
    """Return a list of the downloaded sitemap documents.
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    :param force_fetch: if True, the files will be downloaded from s3
                        even if they are in the tmp directory.
                        if False, files that are present in the tmp_directory
                        will not be downloaded from s3.
    :type force_fetch: bool
    :returns: list
    """
    sitemap_files = download_sitemaps_from_s3(s3_uri, tmp_dir, force_fetch)
    result = []
    for sitemap_file, url in sitemap_files:
        sitemap_document = instanciate_sitemap_document(sitemap_file, url)
        result.append(sitemap_document)
    return result


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
