from enum import Enum
from lxml import etree
import gzip

from cdf.log import logger
from cdf.features.sitemap.exceptions import ParsingError, UnhandledFileType


class SiteMapType(Enum):
    UNKNOWN = 0
    SITEMAP = 1
    SITEMAP_INDEX = 2


def instanciate_sitemap_document(file_path):
    xml_document = SitemapXmlDocument(file_path)
    if xml_document.get_sitemap_type() != SiteMapType.UNKNOWN:
        return xml_document

    rss_document = SitemapRssDocument(file_path)
    if rss_document.get_sitemap_type() != SiteMapType.UNKNOWN:
        return rss_document

    raise UnhandledFileType()


class SitemapXmlDocument(object):
    """A class to represent a sitemap xml document.
    It can represent a sitemap or a sitemap index.
    """
    def __init__(self, file_path):
        """Constructor
        :param file_path: the path to the input file
        :type file_path: str
        """
        self.file_path = file_path

    def get_sitemap_type(self):
        with open_sitemap_file(self.file_path) as f:
            result = guess_sitemap_type(f)
        return result

    def get_urls(self):
        """Returns the urls listed in the sitemap document
        :param file_object: a file like object
        :type file_object: file
        """
        with open_sitemap_file(self.file_path) as file_object:
            try:
                for _, element in etree.iterparse(file_object):
                    localname = etree.QName(element.tag).localname
                    if localname == "loc":
                        yield element.text
                    element.clear()
            except etree.XMLSyntaxError as e:
                raise ParsingError(e.message)


class SitemapRssDocument(object):
    """A class to represent a sitemap rss document.
    """
    def __init__(self, file_path):
        """Constructor
        :param file_path: the path to the input file
        :type file_path: str
        """
        self.file_path = file_path

    def get_sitemap_type(self):
        #rss document cannot be sitemap_index
        return SiteMapType.SITEMAP

    def get_urls(self):
        """Returns the urls listed in the sitemap document
        :param file_object: a file like object
        :type file_object: file
        """
        with open_sitemap_file(self.file_path) as file_object:
            try:
                for _, element in etree.iterparse(file_object):
                    localname = etree.QName(element.tag).localname
                    if localname == "link":
                        yield element.text
                        element.clear()
            except etree.XMLSyntaxError as e:
                raise ParsingError(e.message)


def open_sitemap_file(file_path):
    """Create a file-like object from a path.
    The function handles gzip and plain text files
    :param file_path: the path to the input file
    :type file_path: str
    :returns: file
    """
    try:
        f = gzip.open(file_path)
        #if it is not a gzip file, it will raise here
        f.read()
        f = gzip.open(file_path)
    except:
        f = open(file_path)
    return f


def guess_sitemap_type(file_object):
    """Guess the  sitemap type (sitemap or sitemap index) from an input file.
    The method simply stops on the first "urlset" or "sitemapindex" tag.
    :param file_object: a file like object
    :type file_object: file
    :return: SiteMapType
    """
    try:
        for _, element in etree.iterparse(file_object, events=("start",)):
            localname = etree.QName(element.tag).localname
            element.clear()
            if localname == "urlset":
                return SiteMapType.SITEMAP
            elif localname == "sitemapindex":
                return SiteMapType.SITEMAP_INDEX
    except etree.XMLSyntaxError:
        return SiteMapType.UNKNOWN

    return SiteMapType.UNKNOWN
