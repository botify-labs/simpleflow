from enum import Enum
from lxml import etree
from collections import Counter
import gzip

from cdf.log import logger
from cdf.features.sitemap.exceptions import ParsingError


class SiteMapType(Enum):
    UNKNOWN = 0
    SITEMAP = 1
    SITEMAP_INDEX = 2


class SitemapDocument(object):
    """A class to represent a sitemap document.
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
    """Guess the  sitemap type (sitemap or sitemap index) from an input file
    :param file_object: a file like object
    :type file_object: file
    :return: SiteMapType
    """
    tag_counter = Counter()
    try:
        for _, element in etree.iterparse(file_object):
            localname = etree.QName(element.tag).localname
            tag_counter.update([localname])
            element.clear()
    except etree.XMLSyntaxError:
        return SiteMapType.UNKNOWN

    if is_sitemap_index(tag_counter):
        return SiteMapType.SITEMAP_INDEX
    elif is_sitemap(tag_counter):
        return SiteMapType.SITEMAP
    else:
        return SiteMapType.UNKNOWN


def is_sitemap_index(counter):
    """Determine whether a xml document is a sitemap index or not.
    Document type is naively detected by detecting the presence of
    sitemapindex tag.
    :param xml_doc: the parsed xml document
    :type xml_doc: lxml.etree._ElementTree
    :returns: bool
    """
    return counter["sitemapindex"] == 1


def is_sitemap(counter):
    """Determine whether a xml document is a sitemap index or not.
    Document type is naively detected by detecting the presence of
    urlset tag.
    :param xml_doc: the parsed xml document
    :type xml_doc: lxml.etree._ElementTree
    :returns: bool
    """
    return counter["urlset"] == 1
