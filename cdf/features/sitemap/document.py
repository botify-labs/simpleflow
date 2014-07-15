from enum import Enum
from lxml import etree
import gzip
from abc import ABCMeta, abstractmethod
import csv
import re
from urlparse import urlparse

from cdf.log import logger
from cdf.features.sitemap.exceptions import ParsingError, UnhandledFileType


class SiteMapType(Enum):
    UNKNOWN = 0
    SITEMAP_XML = 1
    SITEMAP_RSS = 2
    SITEMAP_TEXT = 3
    SITEMAP_INDEX = 4

def is_xml_sitemap(sitemap_type):
    return sitemap_type == SiteMapType.SITEMAP_XML

def is_sitemap_index(sitemap_type):
    return sitemap_type == SiteMapType.SITEMAP_INDEX

def is_rss_sitemap(sitemap_type):
    return sitemap_type == SiteMapType.SITEMAP_RSS

def is_text_sitemap(sitemap_type):
    return sitemap_type == SiteMapType.SITEMAP_TEXT

def instanciate_sitemap_document(file_path, url):
    """a factory method that creates a sitemap document from a file
    :param file_path: the input file path
    :type file_path: str
    :param url: the url where the file has been downloaded from
    :type url: str
    :returns: SitemapDocument
    :raises: UnhandledFileType
    """
    sitemap_type = guess_sitemap_type(file_path)
    if is_xml_sitemap(sitemap_type):
        return SitemapXmlDocument(file_path)

    if is_sitemap_index(sitemap_type):
        return SitemapIndexXmlDocument(file_path, url)

    if is_rss_sitemap(sitemap_type):
        return SitemapRssDocument(file_path)

    if is_text_sitemap(sitemap_type):
        return SitemapTextDocument(file_path)

    raise UnhandledFileType()


class SitemapDocument(object):
    """An abstract class to represent a sitemap document.
    It can represent a sitemap or a sitemap index.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        self.valid_urls = 0
        self.invalid_urls = 0

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
        super(AbstractSitemapXml, self).__init__()
        self.file_path = file_path

    def get_urls(self):
        """Returns the urls listed in the sitemap document
        :param file_object: a file like object
        :type file_object: file
        """
        with open_sitemap_file(self.file_path) as file_object:
            try:
                for _, element in etree.iterparse(file_object):
                    if self._is_valid_element(element):
                        url = element.text
                        if self._is_valid_url(url):
                            self.valid_urls += 1
                            yield element.text
                        else:
                            self.invalid_urls += 1
                    element.clear()
            except etree.XMLSyntaxError as e:
                raise ParsingError(e.message)

    @abstractmethod
    def _is_valid_element(self, element):
        """A template method that decides whether or not an xml tree element
        is a valid url element.
        :param element: the element to test.
        :type element: lxml.etree._Element"""
        raise NotImplementedError()

    @abstractmethod
    def _is_valid_url(self, element):
        """A template method that decides whether or not a url is valid.
        :param element: the element to test.
        :type element: lxml.etree._Element"""
        raise NotImplementedError()


class SitemapXmlDocument(AbstractSitemapXml):
    """A class to represent a sitemap xml document.
    It can represent a sitemap or a sitemap index.
    """
    def __init__(self, file_path):
        """Constructor
        :param file_path: the path to the input file
        :type file_path: str
        """
        super(SitemapXmlDocument, self).__init__(file_path)

    def get_sitemap_type(self):
        return SiteMapType.SITEMAP_XML

    def _is_valid_element(self, element):
        """Implementation of the template method for XML sitemaps"""
        localname = etree.QName(element.tag).localname
        if localname != "loc":
            return False
        #check the parent tag, to avoid returning
        #image urls found in image sitemaps
        parent_node = element.getparent()
        parent_localname = etree.QName(parent_node.tag).localname
        return parent_localname == "url"

    def _is_valid_url(self, url):
        """Implementation of the template method for XML sitemaps"""
        return UrlValidator.is_valid(url)


class SitemapIndexXmlDocument(AbstractSitemapXml):
    """A class to represent a sitemap index xml document.
    """
    def __init__(self, file_path, url):
        super(SitemapIndexXmlDocument, self).__init__(file_path)
        self.url = url
        self.sitemap_url_validator = SitemapUrlValidator(self.url)

    def get_sitemap_type(self):
        return SiteMapType.SITEMAP_INDEX

    def _is_valid_element(self, element):
        """Implementation of the template method for sitemap indexes"""
        localname = etree.QName(element.tag).localname
        return localname == "loc"

    def _is_valid_url(self, url):
        """Implementation of the template method for itemap indexes"""
        return UrlValidator.is_valid(url) and self.sitemap_url_validator.is_valid(url)


class SitemapRssDocument(AbstractSitemapXml):
    """A class to represent a sitemap rss document.
    """
    def __init__(self, file_path):
        """Constructor
        :param file_path: the path to the input file
        :type file_path: str
        """
        super(self.__class__, self).__init__(file_path)

    def get_sitemap_type(self):
        #rss document cannot be sitemap_index
        return SiteMapType.SITEMAP_RSS

    def _is_valid_element(self, element):
        """Implementation of the template method for RSS sitemaps"""
        localname = etree.QName(element.tag).localname
        return localname == "link"

    def _is_valid_url(self, url):
        """Implementation of the template method for RSS sitemaps"""
        return UrlValidator.is_valid(url)


class SitemapTextDocument(SitemapDocument):
    """A class to represent a sitemap rss document.
    """
    def __init__(self, file_path):
        """Constructor
        :param file_path: the path to the input file
        :type file_path: str
        """
        super(self.__class__, self).__init__()
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
            csv_reader = csv.reader(file_object, delimiter='\n')
            #do not use a simple "for" loop to be able to catch csv.Error
            #and simply skip the corresponding lines
            while True:
                try:
                    row = csv_reader.next()
                except csv.Error:
                    self.invalid_urls += 1
                    #simply skip the line
                    continue
                except StopIteration:
                    break
                if len(row) != 1:
                    logger.warning("'%s' should have exactly one field.", row)
                    self.invalid_urls += 1
                    continue
                url = row[0]
                if UrlValidator.is_valid(url):
                    self.valid_urls += 1
                    yield row[0]
                else:
                    self.invalid_urls += 1


class UrlValidator(object):
    MAXIMUM_LENGTH = 4096
    #an url is considered valid if it starts with
    #http:// or https:// whatever the case.
    validation_regex = re.compile(r'https?://', re.IGNORECASE)

    @classmethod
    def is_valid(cls, url):
        """Check if a string is a valid url (in a sitemap context)
        :param url: the input string
        :type url: str
        :returns: bool
        """
        if len(url) > cls.MAXIMUM_LENGTH:
            return False
        if cls.validation_regex.match(url) is None:
            return False
        return True


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


def guess_sitemap_type(file_path):
    """Guess the  sitemap type (sitemap or sitemap index) from an input file.
    The method simply stops on the first "urlset" or "sitemapindex" tag.
    :param file_path: the path to the input file
    :type file_path: str
    :return: SiteMapType
    """
    with open_sitemap_file(file_path) as file_object:
        try:
            xml_like = False
            for _, element in etree.iterparse(file_object, events=("start",)):
                xml_like = True  # we were able to parse at least one element
                localname = etree.QName(element.tag).localname
                element.clear()
                if localname == "urlset":
                    return SiteMapType.SITEMAP_XML
                elif localname == "sitemapindex":
                    return SiteMapType.SITEMAP_INDEX
                elif localname == "rss":
                    return SiteMapType.SITEMAP_RSS
        except etree.XMLSyntaxError:
            pass

    if xml_like:
        #it looked like an xml but was not a valid sitemap
        return SiteMapType.UNKNOWN

    text_sitemap = SitemapTextDocument(file_path)
    nb_urls = sum(1 for _ in text_sitemap.get_urls())
    if nb_urls > 0:
        return SiteMapType.SITEMAP_TEXT
    return SiteMapType.UNKNOWN


class SitemapUrlValidator(object):
    """A class that tells if a sitemap_index can reference given urls.
    The rules differ from the standard (they are more flexible):
    - http://www.sitemaps.org/protocol.html#index

    A sitemap index can only reference urls in its domain or its subdomains.
    There is a special case, when the sitemap domain is the "www",
    In this case, the sitemap index can reference all the domains of the site.
    foo.com -> *.foo.com, foo.com
    www.foo.com -> *.www.foo.com, *.foo.com, www.foo.com
    """
    def __init__(self, sitemap_index_url):
        """Constructor
        :param sitemap_index_url: the sitemap index url
        :type sitemap_index_url: str
        """
        parsed_sitemap_index_url = urlparse(sitemap_index_url)
        #the validation rules are only based on the host
        #so we only keep it.
        self.sitemap_index_host = parsed_sitemap_index_url.netloc

    def _is_subdomain(self, subdomain, domain):
        """Helper function that tells if a domain is the subdomain of an other domain
        :param subdomain: the potential subdomain
        :type subdomain: str
        :param domain: the potential domain
        :type domain: str
        :returns: bool"""
        return subdomain.endswith(".{}".format(domain))

    def is_valid(self, sitemap_url):
        """The actual validation function
        :param sitemap_url: the sitemap url to test
        :type sitemap_url: str
        :returns: bool
        """
        sitemap_index_host = self.sitemap_index_host

        parsed_sitemap_url = urlparse(sitemap_url)
        sitemap_host = parsed_sitemap_url.netloc

        #handles the www case
        if sitemap_index_host.startswith("www."):
            #we simply remove the www. and apply the standard rules.
            sitemap_index_host = sitemap_index_host[4:]

        #if the domain is the same, it's ok.
        if sitemap_index_host == sitemap_host:
            return True

        #if the sitemap is on a subdomain, it's ok
        if self._is_subdomain(sitemap_host, sitemap_index_host):
            return True

        return False
