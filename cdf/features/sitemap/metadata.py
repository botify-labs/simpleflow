import json

from cdf.features.sitemap.document import (SiteMapType,
                                           is_xml_sitemap,
                                           is_sitemap_index,
                                           is_rss_sitemap,
                                           is_text_sitemap)


class SitemapMetadata(object):
    """A class to represent sitemap in Metadata
    The class does not contain the document itself
    only basic reporting information about it"""
    def __init__(self, url, sitemap_type, s3_uri, sitemap_indexes=None):
        """Constructor
        :param url: the sitemap url
        :type url: str
        :param sitemap_type: the type of the sitemap: xml, rss, txt
        :type sitemap_type: SiteMapType
        :param s3_uri: the s3_uri where the sitemap is stored
        :type s3_uri: str
        :param sitemap_index: the url of the sitemap index that references the
                              sitemap (if any)
        :type sitemap_index: str
        """
        self.url = url
        self.sitemap_type = sitemap_type
        self.s3_uri = s3_uri
        self.sitemap_indexes = sitemap_indexes or []
        self.error_type = None
        self.error_message = None
        self.valid_urls = None
        self.invalid_urls = None

    def to_dict(self):
        result = {
            "url": self.url,
            "file_type": self.sitemap_type.name,
            "s3_uri": self.s3_uri,
        }

        if self.sitemap_indexes is not None and len(self.sitemap_indexes) > 0:
            result["sitemap_indexes"] = self.sitemap_indexes
        if self.valid_urls is not None:
            result["valid_urls"] = self.valid_urls
        if self.invalid_urls is not None:
            result["invalid_urls"] = self.invalid_urls
        if self.error_type is not None:
            result["error"] = self.error_type
        if self.error_message is not None:
            result["message"] = self.error_message
        return result

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()

    def __repr__(self):
        return repr(self.to_dict())

    def __hash__(self):
        return hash(repr(self))


class SitemapIndexMetadata(object):
    """A class to represent a sitemap index in a Metadata
    The class does not contain the document itself
    only basic reporting information about it"""
    def __init__(self, url, valid_urls, invalid_urls, error_type=None, error_message=None):
        """Constructor
        """
        self.url = url
        self.valid_urls = valid_urls
        self.invalid_urls = invalid_urls
        self.error_type = error_type
        self.error_message = error_message

    def to_dict(self):
        result = {
            "url": self.url,
            "valid_urls": self.valid_urls,
            "invalid_urls": self.invalid_urls
        }
        if self.error_type is not None:
            result["error"] = self.error_type
        if self.error_message is not None:
            result["message"] = self.error_message
        return result

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()

    def __repr__(self):
        return repr(self.to_dict())

    def __hash__(self):
        return hash(repr(self))


class Error(object):
    def __init__(self, url, file_type, error_type, message):
        """Constructor
        :param url: the file url
        :type url: str
        :param file_type: the type of the file
        :type file_type: SiteMapType
        :param error_type: the type of error that occured
        :type error_type: str
        :param message: error message (additional information about the error)
        :type message: str
        """
        self.url = url
        self.file_type = file_type
        self.error_type = error_type
        self.message = message

    def to_dict(self):
        return {
            "url": self.url,
            "file_type": self.file_type.name,
            "error": self.error_type,
            "message": self.message
        }

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()

    def __hash__(self):
        return hash(str(self.to_dict()))

    def __repr__(self):
        return str(self.to_dict())


class Metadata(object):
    """A class information about the downloaded sitemaps:
        where they come from, where they are stored,
        errors that occured
        :param sitemaps: the list of downloaded sitemaps.
                         each sitemap is an instance of Sitemap
        :type sitemaps: list
        :param sitemap_indexes: the list of downloaded sitemap indexes.
        :type sitemap_indexes: list
        :param errors: the list of sitemap errors. Each error is a string
                       representing an url.
        :type errors: list"""
    def __init__(self, sitemaps=None, sitemap_indexes=None, errors=None):
        self.sitemaps = sitemaps or []
        self.sitemap_indexes = sitemap_indexes or []
        self.errors = errors or []

    def add_success_sitemap(self, sitemap_metadata):
        """Add metadata a about sitemap that has been successfuly downloaded.
        If sitemap is already known, do nothing.
        :param sitemap_metadata: the input sitemap
        :type sitemap_metadata: SitemapMetadata
        """
        if not sitemap_metadata.url in [s.url for s in self.sitemaps]:
            self.sitemaps.append(sitemap_metadata)

    def add_success_sitemap_index(self, sitemap_index):
        """Add a sitemap index that has been successfuly downloaded.
        If sitemap index is already known, do nothing.
        :param sitemap_index: the input sitemap_index
        :type sitemap_index: SitemapIndexMetada
        """
        if not sitemap_index.url in [s.url for s in self.sitemap_indexes]:
            self.sitemap_indexes.append(sitemap_index)

    def add_error(self, error):
        """Add an error url
        If url is already known, do nothing.
        :param error: the error
        :type error: Error
        """
        if not error.url in [e.url for e in self.errors]:
            self.errors.append(error)

    def to_json(self):
        """Return a json representation of the object
        :returns: str"""
        d = {
            "sitemaps": [sitemap.to_dict() for sitemap in self.sitemaps],
            "sitemap_indexes": [sitemap_index.to_dict() for sitemap_index in self.sitemap_indexes],
            "errors": [e.to_dict() for e in self.errors]
        }
        return json.dumps(d)

    def __eq__(self, other):
        return (set(self.sitemaps) == set(other.sitemaps) and
                set(self.sitemap_indexes) == set(other.sitemap_indexes) and
                set(self.errors) == set(other.errors))

    def __repr__(self):
        return self.to_json()

    def is_success_sitemap(self, url):
        """Determines whether or not a given url correspond to a successfully
        processed sitemap.
        :param url: the input url
        :type url: str
        :returns: bool
        """
        return url in [sitemap.url for sitemap in self.sitemaps]


def parse_sitemap_metadata(input_dict):
    result = SitemapMetadata(input_dict["url"],
                             SiteMapType[input_dict["file_type"]],
                             input_dict["s3_uri"])
    if "sitemap_indexes" in input_dict:
        result.sitemap_indexes = input_dict["sitemap_indexes"]
    if "valid_urls" in input_dict:
        result.valid_urls = input_dict["valid_urls"]
    if "invalid_urls" in input_dict:
        result.invalid_urls = input_dict["invalid_urls"]
    if "error" in input_dict:
        result.error_type = input_dict["error"]
    if "message" in input_dict:
        result.error_message = input_dict["message"]
    return result


def parse_sitemap_index_metadata(input_dict):
    result = SitemapIndexMetadata(input_dict["url"],
                                  input_dict["valid_urls"],
                                  input_dict["invalid_urls"])

    if "valid_urls" in input_dict:
        result.valid_urls = input_dict["valid_urls"]
    if "invalid_urls" in input_dict:
        result.invalid_urls = input_dict["invalid_urls"]
    if "error" in input_dict:
        result.error_type = input_dict["error"]
    if "message" in input_dict:
        result.error_message = input_dict["message"]
    return result


def parse_error(input_dict):
    url = input_dict["url"]
    file_type = SiteMapType[input_dict["type"]]
    error_type = input_dict["error"]
    error_message = input_dict["message"]
    return Error(url, file_type, error_type, error_message)


def parse_download_status_from_json(file_path):
    """Build a Metadata object from a json file
    :param file_path: the input file path
    :type file_path: str
    :returns: Metadata
    """
    with open(file_path) as f:
        download_status = json.load(f)
    sitemaps = [parse_sitemap_metadata(sitemap_dict) for sitemap_dict in download_status["sitemaps"]]
    sitemap_indexes = [parse_sitemap_index_metadata(s) for s in download_status["sitemap_indexes"]]
    errors = [parse_error(error) for error in download_status["errors"]]
    return Metadata(sitemaps, sitemap_indexes, errors)


