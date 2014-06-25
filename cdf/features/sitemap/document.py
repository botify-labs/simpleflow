from enum import Enum
from lxml import etree
import gzip
from abc import ABCMeta, abstractmethod
import csv

from cdf.log import logger
from cdf.features.sitemap.exceptions import ParsingError, UnhandledFileType


class SiteMapType(Enum):
    UNKNOWN = 0
    SITEMAP_XML = 1
    SITEMAP_RSS = 2
    SITEMAP_TEXT = 2
    SITEMAP_INDEX = 4


def instanciate_sitemap_document(file_path):
    """a factory method that creates a sitemap document from a file
    :param file_path: the input file path
    :type file_path: str
    """
    sitemap_type = guess_sitemap_type(file_path)
    if sitemap_type == SiteMapType.SITEMAP_XML:
        return SitemapXmlDocument(file_path)

    if sitemap_type == SiteMapType.SITEMAP_INDEX:
        return SitemapIndexXmlDocument(file_path)

    if sitemap_type == SiteMapType.SITEMAP_RSS:
        return SitemapRssDocument(file_path)

    raise UnhandledFileType()


class SitemapDocument(object):
    """An abstract class to represent a sitemap document.
    It can represent a sitemap or a sitemap index.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_sitemap_type(self):
        """Return the type of the sitemap document
        :returns: SiteMapType
        """
        raise NotImplementedError()

    @abstractmethod
    def get_urls(self):
        """Returns the urls listed in the sitemap document
        :param file_object: a file like object
        :type file_object: file
        """
        raise NotImplementedError()


class AbstractSitemapXml(SitemapDocument):
    """An abstract class to represent a xml sitemap
    It can represent a sitemap or a sitemap index.
    """
    __metaclass__ = ABCMeta

    def __init__(self, file_path):
        """Constructor
        :param file_path: the path to the input file
        :type file_path: str
        """
        self.file_path = file_path

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


class SitemapXmlDocument(AbstractSitemapXml):
    """A class to represent a sitemap xml document.
    It can represent a sitemap or a sitemap index.
    """
    def get_sitemap_type(self):
        return SiteMapType.SITEMAP_XML


class SitemapIndexXmlDocument(AbstractSitemapXml):
    """A class to represent a sitemap index xml document.
    """
    def get_sitemap_type(self):
        return SiteMapType.SITEMAP_INDEX


class SitemapRssDocument(SitemapDocument):
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
        return SiteMapType.SITEMAP_RSS

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


class SitemapTextDocument(SitemapDocument):
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
        return SiteMapType.SITEMAP_TEXT

    def get_urls(self):
        """Returns the urls listed in the sitemap document
        :param file_object: a file like object
        :type file_object: file
        """
        with open_sitemap_file(self.file_path) as file_object:
            csv_reader = csv.reader(file_object)
            for row in csv_reader:
                if len(row) != 1:
                    logger.warning("'%s' should have only one field.", row)
                #we do not check if the string looks like an url
                #(we don't do it on xml and rss sitemaps)
                yield row[0]


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
                return SiteMapType.SITEMAP_XML
            elif localname == "sitemapindex":
                return SiteMapType.SITEMAP_INDEX
            elif localname == "rss":
                return SiteMapType.SITEMAP_RSS
    except etree.XMLSyntaxError:
        return SiteMapType.UNKNOWN

    return SiteMapType.UNKNOWN
