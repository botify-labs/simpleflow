import os.path
import gzip

from cdf.utils import s3
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir
from cdf.features.main.utils import get_url_to_id_dict_from_stream

from cdf.features.main.streams import IdStreamDef
from cdf.core.constants import FIRST_PART_ID_SIZE, PART_ID_SIZE
from cdf.features.sitemaps.constant import NB_SAMPLES_TO_KEEP
from cdf.features.sitemaps.download import download_sitemaps
from cdf.features.sitemaps.metadata import Metadata, SitemapMetadata
from cdf.features.sitemaps.streams import SitemapStreamDef
from cdf.features.sitemaps.matching import (match_sitemap_urls_from_documents,
                                           get_download_metadata_from_s3,
                                           get_sitemap_documents,
                                           DomainValidator)


@with_temporary_dir
def download_sitemap_files(input_urls,
                           s3_uri,
                           user_agent,
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
    :type user_agent: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    """
    s3_download_metadata = Metadata()
    for url in input_urls:
        download_sitemap_file(url,
                              s3_uri,
                              user_agent,
                              s3_download_metadata,
                              tmp_dir,
                              force_fetch)

    s3_subdir_uri = os.path.join(s3_uri, "sitemaps")

    #push the file that list the sitemap files
    s3.push_content(
        os.path.join(s3_subdir_uri, "sitemap_download_metadata.json"),
        s3_download_metadata.to_json()
    )


def download_sitemap_file(input_url,
                          s3_uri,
                          user_agent,
                          metadata,
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
    :type user_agent: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    """
    download_sitemaps(input_url, tmp_dir, user_agent, metadata)
    s3_subdir_uri = os.path.join(s3_uri, "sitemaps")
    #an object similar to download_metadata but that stores s3 uris
    for sitemap in metadata.sitemaps:
        url, file_path, sitemap_indexes = sitemap.url, sitemap.s3_uri, sitemap.sitemap_indexes
        if not sitemap.s3_uri.startswith("s3://"):
            #The file has not been pushed to s3 yet
            destination_uri = os.path.join(s3_subdir_uri, os.path.basename(file_path))
            s3.push_file(
                os.path.join(destination_uri),
                file_path
            )
            sitemap.s3_uri = destination_uri


@with_temporary_dir
def match_sitemap_urls(s3_uri,
                       allowed_domains,
                       blacklisted_domains,
                       first_part_id_size=FIRST_PART_ID_SIZE,
                       part_id_size=PART_ID_SIZE,
                       tmp_dir=None,
                       force_fetch=False):
    """Match urls from the sitemaps to urls from the crawl.
    When the url is present in the crawl, we save its urlid in a file
    'sitemap.txt.XXX.gz', when it is not present, the full url is
    saved in a file 'sitemap_only.gz'.
    The generated files are then pushed to s3.
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param allowed_domains: the list domains that we are allowed to crawl.
                            Each element of the list is either an literal string
                            for an allowed domain or a string with some "*"
                            wildcards that can be replaced by anything
    :type allowed_domains: list
    :param blacklisted_domains: a list of domains that we are
                                not allowed to crawl.
                                Wildcards are not allowed for blacklisted domains.
    :type blacklisted_domains: list
    :param first_part_id_size: the size of the first part
    :type first_part_id_size: int
    :param part_id_size: the size of all other parts
    :type part_id_size: int
    :param tmp_dir: the directory where to save temporary data
    """
    #load crawl information
    id_stream = IdStreamDef.get_stream_from_s3(s3_uri, tmp_dir=tmp_dir)
    url_to_id = get_url_to_id_dict_from_stream(id_stream)
    #download sitemaps

    domain_validator = DomainValidator(allowed_domains, blacklisted_domains)
    dataset = SitemapStreamDef.create_temporary_dataset()

    sitemap_documents = get_sitemap_documents(s3_uri, tmp_dir, force_fetch)
    sitemap_only_urls = []
    out_of_crawl_domain_urls = []
    match_sitemap_urls_from_documents(
        sitemap_documents,
        url_to_id,
        dataset,
        domain_validator,
        NB_SAMPLES_TO_KEEP,
        sitemap_only_urls,
        out_of_crawl_domain_urls)

    dataset.persist_to_s3(s3_uri,
                          first_part_id_size=first_part_id_size,
                          part_id_size=part_id_size)

    download_metadata = get_download_metadata_from_s3(s3_uri, tmp_dir, force_fetch)
    update_download_status(download_metadata, sitemap_documents)

    sitemap_metadata_filename = "sitemap_metadata.json"
    sitemap_metadata_filepath = os.path.join(tmp_dir, sitemap_metadata_filename)
    with open(sitemap_metadata_filepath, 'wb') as sitemap_metadata_file:
        sitemap_metadata_file.write(download_metadata.to_json())
    s3.push_file(
        os.path.join(s3_uri, sitemap_metadata_filename),
        sitemap_metadata_filepath
    )
    sitemap_only_filename = 'sitemap_only.gz'
    sitemap_only_filepath = save_url_list_as_gzip(sitemap_only_urls,
                                                  sitemap_only_filename,
                                                  tmp_dir)
    s3.push_file(
        os.path.join(s3_uri, sitemap_only_filename),
        sitemap_only_filepath
    )

    out_of_crawl_domain_filename = 'in_sitemap_out_of_crawl_domain.gz'
    out_of_crawl_domain_filepath = save_url_list_as_gzip(out_of_crawl_domain_urls,
                                                         out_of_crawl_domain_filename,
                                                         tmp_dir)
    s3.push_file(
        os.path.join(s3_uri, out_of_crawl_domain_filename),
        out_of_crawl_domain_filepath
    )


def update_download_status(download_status, sitemap_documents):
    """Update a Metadata object with data obtained when extracting
    the urls from the sitemaps.
    This function basically fills the "valid_url", "invalid_urls" fields
    for the sitemaps. It also fills the error related fields if necessary.
    :param download_status: the download status object to update
    :type download_status: Metadata
    :param sitemap_documents: a list of sitemap documents.
                              They contain the information to update
                              the download status
    :type sitemap_documents: list
    """
    url_to_metadata = {
        sitemap_metadata.url: sitemap_metadata for sitemap_metadata in download_status.sitemaps
    }
    for document in sitemap_documents:
        document_metadata = url_to_metadata[document.url]
        document_metadata.valid_urls = document.valid_urls
        document_metadata.invalid_urls = document.invalid_urls
        if document.error is not None:
            document_metadata.error_type = document.error
        if document.error_message is not None:
            document_metadata.error_message = document.error_message


def save_url_list_as_gzip(url_list, filename, tmp_dir):
    """Save a list of urls in a gzip file.
    Each line contains one url.
    :param url_list: the url list
    :type url_list: list
    :param filename: the name of the file to create
    :type filename: str
    :param tmp_dir: the dir where to create the file
    :type tmp_dir: str
    :returns: str - the path to the created file
    """
    local_filepath = os.path.join(tmp_dir, filename)
    with gzip.open(local_filepath, 'wb') as local_file:
        for url in url_list:
            #use "+" instead of "format" since
            #ampelmann has benchmarked both methods
            #and found the "+" is almost twice faster.
            local_file.write(unicode(url + "\n").encode("utf-8"))
    return local_filepath

