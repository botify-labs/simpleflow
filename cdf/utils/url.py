from urlparse import urlparse
from tld import get_tld


def get_domain(url):
    """Extract the domain from an url
    :param url: the input url
    :type url: str
    :rtype: str
    """
    parsed_url = urlparse(url)
    return parsed_url.netloc


def get_second_level_domain(url):
    """Extract the second level domain from an url.
    For instance :
        - analytics.twitter.com -> twitter.com
        - international.blog.bbc.co.ul -> bbc.co.uk
    :param url: the input url
    :type url: str
    :rtype: str
    """
    #tld gets the job done
    #(even if the function name is misleading)
    return get_tld(url)
