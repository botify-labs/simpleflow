from itertools import groupby, ifilter, imap, ifilterfalse
import heapq
from cdf.analysis.urls.utils import get_url_id, get_es_id

from cdf.features.links.helpers.predicates import (
    is_link,
    is_link_internal,
    is_follow_link
)
from cdf.utils.es import multi_get
from cdf.utils.url import get_domain, get_second_level_domain
from cdf.utils.external_sort import external_sort
from cdf.core.streams.cache import MarshalStreamCache
from cdf.exceptions import InvalidUrlException
from cdf.features.links.streams import OutlinksRawStreamDef


FOLLOW_SAMPLES = 'follow_samples'
NOFOLLOW_SAMPLES = 'nofollow_samples'
DOMAIN = 'domain'
UNIQUE_FOLLOW_LINKS = 'unique_follow_links'
FOLLOW_LINKS = 'follow_links'
NOFOLLOW_LINKS = 'nofollow_links'


class DomainLinkStats(object):
    """Stats of external outgoing links to a certain domain"""
    def __init__(self,
                 name,
                 follow,
                 nofollow,
                 follow_unique,
                 sample_follow_links=None,
                 sample_nofollow_links=None):
        """Constructor
        :param name: the domain name
        :type name: str
        :param follow: the number of follow links to the domain
        :type follow: int
        :param nofollow: the number of nofollow links to the domain
        :type nofollow: int
        :param follow_unique: the number of unique follow links to the domain
        :type follow_unique: int
        :param sample_follow_links: a list of sample follow link destination
                             (as a list of LinkDestination)
        :param sample_nofollow_links: a list of sample nofollow link destination
                             (as a list of LinkDestination)
        :type sample_nofollow_links: list
        """
        self.name = name
        self.follow = follow
        self.nofollow = nofollow
        self.follow_unique = follow_unique
        self.sample_follow_links = sample_follow_links or []
        self.sample_nofollow_links = sample_nofollow_links or []

    def extract_ids(self):
        """Extract url ids from all samples, de-duplication is applied
        """
        ids = set()
        for sample in self.sample_follow_links:
            ids.update(sample.extract_ids())
        for sample in self.sample_nofollow_links:
            ids.update(sample.extract_ids())
        return ids

    def replace_ids(self, id_to_url):
        """Replace url id by its corresponding url in every sample

        :param id_to_url: url_id -> url lookup
        :type id_to_url: dict
        """
        for sample in self.sample_follow_links:
            sample.replace_ids(id_to_url)
        for sample in self.sample_nofollow_links:
            sample.replace_ids(id_to_url)

    def to_dict(self):
        #key function to sort the sample links by number of links
        #and then alphabetically
        key = lambda x: (x.unique_links, x.url)
        return {
            DOMAIN: self.name,
            UNIQUE_FOLLOW_LINKS: self.follow_unique,
            FOLLOW_LINKS: self.follow,
            NOFOLLOW_LINKS: self.nofollow,
            FOLLOW_SAMPLES: [
                sample_link.to_dict() for sample_link in
                sorted(self.sample_follow_links, key=key)
            ],
            NOFOLLOW_SAMPLES: [
                sample_link.to_dict() for sample_link in
                sorted(self.sample_nofollow_links, key=key)
            ]
        }

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()

    def __repr__(self):
        return "DomainLinkStats({})".format(self.to_dict())


class LinkDestination(object):
    """A class to represent a link destination.
    The link destination is defined by :
        - its destination urls
        - the number of unique links that point to it
        - a sample of source urlids.
    """
    def __init__(self, destination_url, unique_links, sample_sources):
        """Constructor
        :param destination_url: the destination url
        :type: str
        :param unique_links: the number of unique links that point to
                             the destination url
        :type unique_links: int
        :param sample_sources: a list of sample source urlids.
        :type sample_source: list
        """
        self.url = destination_url
        self.unique_links = unique_links
        self.sample_sources = sample_sources

    def __eq__(self, other):
        return (
            self.url == other.url and
            self.unique_links == other.unique_links and
            self.sample_sources == other.sample_sources
        )

    def __repr__(self):
        return "{}: {}, {}".format(self.url,
                                   self.unique_links,
                                   self.sample_sources)

    def extract_ids(self):
        """Extract url ids
        """
        return self.sample_sources

    def replace_ids(self, id_to_url):
        """Replace url ids with its corresponding url

        :param id_to_url: url_id -> url lookup
        :type id_to_url: dict
        """
        self.sample_sources = [
            id_to_url[i]
            for i in self.sample_sources
        ]

    def to_dict(self):
        """Return a dict representation of the object
        :rtype: dict
        """
        return {
            "url": self.url,
            "unique_links": self.unique_links,
            "sources": self.sample_sources
        }


def filter_external_outlinks(outlinks):
    """Filter outlinks stream for external, <a> links

    :param outlinks: stream of OutLinksRawStreamDef
    :return: external, <a> outlinks stream
    """
    mask_idx = OutlinksRawStreamDef.field_idx('bitmask')
    dest_idx = OutlinksRawStreamDef.field_idx('dst_url_id')
    type_idx = OutlinksRawStreamDef.field_idx('link_type')
    # filter <a> links
    filtered = ifilter(
        lambda l: is_link(l[type_idx]),
        outlinks
    )
    # filter external outgoing links
    filtered = ifilter(
        lambda l: not is_link_internal(
            l[mask_idx], l[dest_idx], is_bitmask=True),
        filtered
    )
    return filtered


def filter_invalid_destination_urls(outlinks):
    """Remove external outlinks for which the destination url is invalid.
    The destination is declared as invalid.
    if we cannot extract the second level domain from it.
    For instance these urls are declared as invalid:
        - "http://iballisticsquid/",
        - "http://http//www.rueducommerce.fr/selection/23217"
    :param outlinks: the stream of external outlinks
                     (based on OutLinksRawStreamDef)
    :type outlinks: iterator
    :rtype: iterator.
    """
    external_url_idx = OutlinksRawStreamDef.field_idx('external_url')
    def is_valid(x):
        try:
            get_second_level_domain(x[external_url_idx])
            return True
        except InvalidUrlException:
            return False
    return ifilter(is_valid, outlinks)


def _group_links(link_stream, key):
    """A helper function to group elements of a outlink stream
    according to a generic criterion.
    It returns tuples (key_value, corresponding links)
    :param link_stream: the input outlink stream from OutlinksRawStreamDef
                        (should contains only outlinks,
                        no inlinks, no canonical)
    :param link_stream: iterable
    """
    #sort links by key function
    link_stream = external_sort(link_stream, key=key)
    #group by key function
    for key_value, link_group in groupby(link_stream, key=key):
        yield key_value, link_group


def count_unique_links(external_outlinks):
    """Count the number of unique links in a set of external outlinks.
    i.e. if a link to B occurs twice in page A, it is counted only once.
    :param external_outlinks: the input stream of external outlinks
                              (based on OutlinksRawStreamDef)
    :type external_outlinks: iterable
    :rtype: int
    """
    #remove duplicate links
    id_index = OutlinksRawStreamDef.field_idx("id")
    external_url_index = OutlinksRawStreamDef.field_idx("external_url")
    external_outlinks = imap(
        lambda x: (x[id_index], x[external_url_index]),
        external_outlinks
    )
    result = len(set(external_outlinks))
    return result


def count_unique_follow_links(external_outlinks):
    """Count the number of unique follow links in a set of external outlinks.
    i.e. if a link to B occurs twice in page A, it is counted only once.
    :param external_outlinks: the input stream of external outlinks
                              (based on OutlinksRawStreamDef)
    :type external_outlinks: iterable
    :rtype: int
    """
    bitmask_index = OutlinksRawStreamDef.field_idx("bitmask")
    #compute number of unique follow links
    external_follow_outlinks = ifilter(
        lambda x: is_follow_link(x[bitmask_index], is_bitmask=True),
        external_outlinks
    )
    return count_unique_links(external_follow_outlinks)


def _compute_top_full_domains(external_outlinks, n, key):
    """A helper function to compute the top n domains given a custom criterion.
    For each destination domain the function counts the number of unique follow
    links that points to it and use this number to select the top n domains.
    The method returns a list of tuple (nb unique follow links, domain)
    Elements are sorted by decreasing number of unique follow links
    :param external_outlinks: the input stream of external outlinks
                              (based on OutlinksRawStreamDef)
    :type external_outlinks: iterable
    :param n: the maximum number of domains we want to return
    :type n: int
    :param key: the function that extracts the domain from an entry from
                external_outlinks.
    :type key: func
    :rtype: list
    """
    nb_samples = 100
    heap = []
    for domain, link_group in _group_links(external_outlinks, key):

        stream_cache = MarshalStreamCache()
        stream_cache.cache(link_group)
        nb_unique_follow_links = count_unique_follow_links(
            stream_cache.get_stream()
        )

        if nb_unique_follow_links == 0:
            #we don't want to return domain with 0 occurrences.
            continue

        if len(heap) < n:
            domain_stats = compute_domain_stats(
                (domain, stream_cache.get_stream()),
                nb_samples
            )
            heapq.heappush(heap, (nb_unique_follow_links, domain_stats))
        else:
            min_value = heap[0][0]
            if nb_unique_follow_links < min_value:
                #avoid useless pushpop()
                continue
            domain_stats = compute_domain_stats(
                (domain, stream_cache.get_stream()),
                nb_samples
            )
            heapq.heappushpop(heap, (nb_unique_follow_links, domain_stats))
    #back to a list
    result = []
    while len(heap) != 0:
        nb_unique_follow_links, domain = heapq.heappop(heap)
        result.append((nb_unique_follow_links, domain))
    #sort by decreasing number of links
    result.reverse()
    return result


def compute_top_full_domains(external_outlinks, n):
    """A helper function to compute the top n domains.
    For each destination domain the function counts the number of unique follow
    links that points to it and use this number to select the top n domains.
    The method returns a list of tuple (nb unique follow links, domain)
    Elements are sorted by decreasing number of unique follow links
    :param external_outlinks: the input stream of external outlinks
                              (based on OutlinksRawStreamDef)
    :type external_outlinks: iterable
    :param n: the maximum number of domains we want to return
    :type n: int
    :param key: the function that extracts the domain from an entry from
                external_outlinks.
    :type key: func
    :rtype: list
    """
    external_url_idx = OutlinksRawStreamDef.field_idx("external_url")
    key = lambda x: get_domain(x[external_url_idx])
    return _compute_top_full_domains(external_outlinks, n, key)


def compute_top_second_level_domains(external_outlinks, n):
    """A helper function to compute the top n second level domains.
    The method is very similar to "compute_top_n_domains()" but it consider
    "doctissimo.fr" and "forum.doctissimo.fr" as the same domain
    while "compute_top_n_domains()" consider them as different.
    :param external_outlinks: the input stream of external outlinks
                              (based on OutlinksRawStreamDef)
    :type external_outlinks: iterable
    :param n: the maximum number of domains we want to return
    :type n: int
    :param key: the function that extracts the domain from an entry from
                external_outlinks.
    :type key: func
    :rtype: list
    """
    external_url_idx = OutlinksRawStreamDef.field_idx("external_url")
    key = lambda x: get_second_level_domain(x[external_url_idx])
    return _compute_top_full_domains(external_outlinks, n, key)


def compute_domain_stats(grouped_outlinks, nb_samples):
    """Compute full stats out of outlinks of a specific domain
    :param grouped_outlinks: grouped qualified outlinks of a certain domain
        eg: (domain_name, [link1, link2, ...])
    :type grouped_outlinks: tuple
    :param nb_samples: the number of sample links to return
    :type nb_samples: int
    :return: stats of outlinks that target the domain
    :rtype: dict
    """
    domain, outlinks = grouped_outlinks

    stream_cache = MarshalStreamCache()
    stream_cache.cache(outlinks)
    domain_stats = compute_domain_link_counts((domain, stream_cache.get_stream()))
    bitmask_index = OutlinksRawStreamDef.field_idx("bitmask")

    key = lambda x: is_follow_link(x[bitmask_index], is_bitmask=True)
    follow_outlinks = ifilter(key, stream_cache.get_stream())
    domain_stats.sample_follow_links = compute_sample_links(follow_outlinks,
                                                            nb_samples)

    nofollow_outlinks = ifilterfalse(key, stream_cache.get_stream())
    domain_stats.sample_nofollow_links = compute_sample_links(nofollow_outlinks,
                                                              nb_samples)
    return domain_stats


def compute_domain_link_counts(grouped_outlinks):
    """Given a set of external outlinks that point to the same domain,
    compute various link counts:
        - follow links
        - nofollow links
        - unique follow links

    :param grouped_outlinks: grouped qualified outlinks of a certain domain
        eg: (domain_name, [link1, link2, ...])
    :type grouped_outlinks: tuple
    :return: stats of outlinks that target the domain
    :rtype: dict
    """
    # counters
    follow = 0
    nofollow = 0
    follow_unique = 0

    # indices
    mask_idx = OutlinksRawStreamDef.field_idx('bitmask')
    external_url_idx = OutlinksRawStreamDef.field_idx('external_url')
    src_id_idx = OutlinksRawStreamDef.field_idx('id')

    seen_urls = set()
    domain_name, links = grouped_outlinks
    for link in links:
        is_follow = is_follow_link(link[mask_idx], is_bitmask=True)
        dest_url = link[external_url_idx]
        src_id = link[src_id_idx]

        if is_follow:
            follow += 1
            if (src_id, dest_url) not in seen_urls:
                follow_unique += 1
            # add to seen set
            seen_urls.add((src_id, dest_url))
        else:
            nofollow += 1

    return DomainLinkStats(domain_name, follow, nofollow, follow_unique)


def compute_link_destination_stats(links, external_url, nb_source_samples):
    """Given a list of external outlinks that point to the same url,
    count the number of unique links and return a sample of source urlid.
    :param links: the input stream of external outlinks
                  (based on OutlinksRawStreamDef).
                  They all point to the same domain.
    :type links: iterable
    :param external_url: the destination url
    :type external_url: str
    :param nb_source_samples: the number of source urlids
                              to return as a sample.
    :type nb_source_samples: int
    :rtype: LinkDestination
    """
    id_index = OutlinksRawStreamDef.field_idx("id")
    #build the set of source urlids
    link_set = set()
    for link in links:
        link_set.add(link[id_index])

    nb_unique_links = len(link_set)
    sample_sources = sorted(link_set)[:nb_source_samples]

    link_sample = LinkDestination(
        external_url,
        nb_unique_links,
        sample_sources
    )
    return link_sample


def compute_sample_links(external_outlinks, n):
    """Compute sample links from a set of external outlinks that point
    to the same domain.
    The method select the n most linked urls (via the number of unique links)
    For each of the most linked urls, it reports: the url, the number of unique
    links, 3 source urlids.
    The function returns a list of LinkDestination.
    :param external_outlinks: the input stream of external outlinks
                              (based on OutlinksRawStreamDef).
                              They all point to the same domain.
    :type external_outlinks: iterable
    :param n: the maximum number of sample links to return
    :type n: int
    :rtype: list
    """
    external_url_idx = OutlinksRawStreamDef.field_idx("external_url")
    external_outlinks = external_sort(external_outlinks, key=lambda x: x[external_url_idx])
    heap = []
    for external_url, links in groupby(external_outlinks, key=lambda x: x[external_url_idx]):
        nb_source_samples = 3
        link_sample = compute_link_destination_stats(
            links,
            external_url,
            nb_source_samples
        )
        nb_unique_links = link_sample.unique_links
        if len(heap) < n:
            heapq.heappush(heap, (nb_unique_links, link_sample))
        else:
            heapq.heappushpop(heap, (nb_unique_links, link_sample))

    #back to a list
    result = []
    while len(heap) != 0:
        nb_unique_links, external_url = heapq.heappop(heap)
        result.append(external_url)
    #sort by decreasing number of links
    result.reverse()
    return result


def resolve_sample_url_id(es_client, index, doc_type, crawl_id, results):
    """Resolve and in-place replace url_id in the samples

    :param results: top domains analysis results (DomainLinkStats)
    :type results: list
    :return: top domain analysis results with all sample url_ids replaced
    by their corresponding url
    :rtype: list
    """
    url_ids = set()

    # collect url_ids
    for domain_stats in results:
        url_ids.update(domain_stats.extract_ids())

    url_ids = [get_es_id(crawl_id, i) for i in url_ids]

    # resolve using ES
    resolved = multi_get(
        es_client, index, doc_type,
        ids=url_ids, fields=['url'],
        routing=crawl_id
    )

    id_to_url = {
        get_url_id(es_id): doc['url']
        for es_id, doc, found in resolved if found
    }

    for domain_stats in results:
        domain_stats.replace_ids(id_to_url)

    return results
