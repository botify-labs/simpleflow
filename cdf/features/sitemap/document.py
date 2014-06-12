from enum import Enum
from lxml import etree

from cdf.log import logger
from cdf.features.sitemap.exceptions import UnhandledFileType, ParsingError


class SiteMapType(Enum):
    SITEMAP = 1
    SITEMAP_INDEX = 2


class SiteMapDocument(object):
    """A class to represent a sitemap document.
    It can represent a sitemap or a sitemap index.
    """
    def __init__(self, sitemap_type, xml_doc):
        """Constructor
        :param sitemap_type: the document type: sitemap or sitemap_index
        :type sitemap_type: SiteMapType
        :param xml_doc: the xml document corresponding to the sitemap
        :type xml_doc: lxml.etree._ElementTree
        """
        self.type = sitemap_type
        self.xml_doc = xml_doc

    def get_urls(self):
        """Returns the urls listed in the sitemap document"""
        namespaces = get_namespace_dict(self.xml_doc)
        if namespaces is not None:
            loc_elements = self.xml_doc.xpath("//ns:loc", namespaces=namespaces)
        else:
            loc_elements = self.xml_doc.xpath("//loc")

        #no need to unescape xml entities, lxml does it for us
        return [loc_elt.text for loc_elt in loc_elements]


def parse_sitemap_file(file_path):
    """Parse a sitemap file and returns a SiteMapDocument.
    The input file can be a sitemap or a sitemap index.
    It can be plain text or gzipped.
    :param file_path: the path to the input file or a file like object
    :type file_path: str
    :returns: SiteMapDocument
    :raises: ParsingError, UnhandledFileType
    """
    logger.debug("Parsing %s", file_path)
    #etree.parse manages both text files and gzipped files
    try:
        xml_doc = etree.parse(file_path)
    except etree.XMLSyntaxError as e:
        raise ParsingError(e.msg)
    if is_sitemap(xml_doc):
        return SiteMapDocument(SiteMapType.SITEMAP, xml_doc)
    elif is_sitemap_index(xml_doc):
        return SiteMapDocument(SiteMapType.SITEMAP_INDEX, xml_doc)
    else:
        raise UnhandledFileType("{} is not recognized as sitemap.".format(file_path))


def is_sitemap_index(xml_doc):
    namespaces = get_namespace_dict(xml_doc)
    if namespaces is not None:
        sitemap_index_elts = xml_doc.xpath("/ns:sitemapindex", namespaces=namespaces)
    else:
        sitemap_index_elts = xml_doc.xpath("/sitemapindex")
    #document type is naively detected by detecting the presence of sitemapindex
    return len(sitemap_index_elts) == 1


def is_sitemap(xml_doc):
    namespaces = get_namespace_dict(xml_doc)
    if namespaces is not None:
        urlset_elts = xml_doc.xpath("/ns:urlset", namespaces=namespaces)
    else:
        urlset_elts = xml_doc.xpath("/urlset")
    #document type is naively detected by detecting the presence of urlset
    return len(urlset_elts) == 1


def get_namespace_dict(xml_doc):
    """Helper function to build a dict to be used
    as namespaces parameter of xpath() method"""
    if None in xml_doc.getroot().nsmap:
        return {'ns': xml_doc.getroot().nsmap[None]}
    else:
        return None
