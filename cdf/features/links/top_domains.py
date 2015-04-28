from itertools import groupby, ifilter, ifilterfalse
import logging
import heapq
import os
import struct
from collections import Counter
from cdf.utils.kvstore import LevelDB, will_destroy

from cdf.features.links.helpers.predicates import (
    is_link,
    is_follow_link,
    is_external_link
)
from cdf.utils.url import get_domain, get_second_level_domain
from cdf.utils.external_sort import external_sort
from cdf.core.streams.cache import BufferedStreamCache
from cdf.exceptions import InvalidUrlException
from cdf.features.main.utils import get_id_to_url_dict_from_stream
from cdf.features.links.streams import OutlinksRawStreamDef


FOLLOW_SAMPLES = 'follow_samples'
NOFOLLOW_SAMPLES = 'nofollow_samples'
DOMAIN = 'domain'
UNIQUE_FOLLOW_LINKS = 'unique_follow_links'
FOLLOW_LINKS = 'follow_links'
UNIQUE_NOFOLLOW_LINKS = 'unique_nofollow_links'
NOFOLLOW_LINKS = 'nofollow_links'

SRC_IDX = 0
MASK_IDX = 1
URL_IDX = 2


logger = logging.getLogger(__name__)


class DomainLinkStats(object):
    """Stats of external outgoing links to a certain domain"""
    def __init__(self,
                 name,
                 follow,
                 nofollow,
                 follow_unique,
                 nofollow_unique,
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
        :param nofollow_unique: the number of unique nofollow links to the domain
        :type nofollow_unique: int
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
        self.nofollow_unique = nofollow_unique
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
        #key function to sort the sample links by decreasing number of links
        #and then alphabetically
        key = lambda x: (-x.unique_links, x.url)
        return {
            DOMAIN: self.name,
            UNIQUE_FOLLOW_LINKS: self.follow_unique,
            FOLLOW_LINKS: self.follow,
            UNIQUE_NOFOLLOW_LINKS: self.nofollow_unique,
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
            sorted(self.sample_sources) == sorted(other.sample_sources)
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
        lambda l: is_external_link(l[mask_idx]),
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


def remove_unused_columns(outlinks):
    """Remove un-used columns in outlinks stream, keep only top_domain related
    columns in the stream

    :param outlinks: outlinks stream (OutLinksRawStreamDef)
    :type outlinks: iterator
    :return: stream of (src, mask, url)
    :rtype: iterator
    """
    for src, _, mask, _, url in outlinks:
        yield (src, mask, url)


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


# format: src_id(int), is_follow(bool), count(int)
packer = struct.Struct(format='I?I')


def _pre_aggregate_link_stream(filtered_link_stream):
    """Helper to pre-aggregate outlinks from the same url

    :param filtered_link_stream: filtered external outlinks, (src, mask, url)
    :type filtered_link_stream: iterator
    :return: pre-aggregated stream, (src, follow, url, count)
    :rtype: iterator
    """
    for src_id, link_group in groupby(
            filtered_link_stream, lambda x: x[SRC_IDX]):
        # pre aggregation counter for all links in a single url page
        counter = Counter()
        for _, mask, url in link_group:
            is_follow = is_follow_link(mask, is_bitmask=True)
            counter[(is_follow, url)] += 1

        for key in counter.keys():
            is_follow, url = key
            yield src_id, is_follow, url, counter[key]


def _encode_leveldb_stream(pre_aggregated_stream):
    """Encode pre-aggregated outlink stream in string format

    :param pre_aggregated_stream: stream of (src, follow, url, count)
    :type pre_aggregated_stream: iterator
    :return: stream of string
    :rtype: iterator
    """
    p = packer
    for src_id, is_follow, url, count in pre_aggregated_stream:
        try:
            sld = get_second_level_domain(url)
        except InvalidUrlException:
            # skip mal formed urls
            continue

        # encode key
        num_part = p.pack(src_id, is_follow, count)
        key = '\0'.join((sld, url, num_part))

        # all information is in key, value is omitted
        yield key, ''


def _decode_leveldb_stream(db):
    """Decode sorted data stream from intermediate levelDB

    :param db: levelDB instance
    :type db: LevelDB
    :return: decoded stream, (domain, url, src, is_follow, count)
    :rtype: iterator
    """
    p = packer
    for line, _ in db.iterator():
        domain, url, num_part = line.split('\0', 2)
        src, is_follow, count = p.unpack(num_part)
        yield domain, url, src, is_follow, count


def _get_stream_cache(stream):
    """Helper to cache a stream"""
    cache = BufferedStreamCache()
    cache.cache(stream)
    return cache


class TopDomainAggregator(object):
    """Aggregator abstraction that takes groups of pre-aggregated external
    outlinks information, then compute the statistics of `top_domain`
    """
    def __init__(self, n, nb_samples=100):
        """Constructor

        :param n: number of top domains to keep
        :type n: int
        :param nb_samples: number of url samples per top_domain
        :type nb_samples: int
        """
        self.n = n
        self.heap = []
        self.nb_samples = nb_samples

    def _compute_link_counts(self, domain, group_stream):
        """Given a group of pre-aggregated that point to the same domain,
        compute various link counts:
            - follow links
            - nofollow links

        :param domain: the domain
        :type domain: str
        :param group_stream: grouped pre-aggregated links of a certain
            domain, eg: [link1, link2, ...]
        :type group_stream: iterator
        :return: stats of outlinks that target the domain
        :rtype: dict
        """
        follow = 0
        nofollow = 0
        follow_unique = 0
        nofollow_unique = 0
        for _, _, _, is_follow, count in group_stream:
            if is_follow:
                follow += count
                follow_unique += 1
            else:
                nofollow += count
                nofollow_unique += 1
        return DomainLinkStats(
            domain, follow, nofollow,
            follow_unique, nofollow_unique
        )

    def _compute_sample_links(self, group_stream, n):
        """Given a group of pre-aggregated outlinks, returns n sample urls
        that have received the most unique links

        :param group_stream: link group stream
        :type group_stream: iterator
        :param n: the number of sample urls
        :type n: int
        """
        heap = []
        # stream is also sorted on `url` part for a given domain
        for url, group in groupby(group_stream, lambda x: x[1]):
            srcs = {i[2] for i in group}
            nb_unique_links = len(srcs)
            dest = LinkDestination(url, nb_unique_links, sorted(srcs)[:3])
            if len(heap) < n:
                heapq.heappush(heap, (nb_unique_links, dest))
            else:
                heapq.heappushpop(heap, (nb_unique_links, dest))

        dests = []
        while len(heap) != 0:
            nb_unique_links, dest_stats = heapq.heappop(heap)
            dest_stats.sample_sources.sort()
            dests.append(dest_stats)
        dests.reverse()
        return dests

    def _compute_sample_sets(self, group_stream_cache):
        """Compute sample links from a group of pre-aggregated outlinks that point
        to the same domain.
        The method select the n most linked urls (via the number of unique links)
        For each of the most linked urls, it reports: the url, the number
        of unique links, 3 source urlids.
        The function returns a list of LinkDestination.

        :param group_stream_cache: cached stream of a link group
        :type group_stream_cache: BufferedStreamCache
        :return: follow and no-follow link samples
        :rtype: tuple
        """
        # TODO extract global variable
        follow_idx = 3
        filter_func = lambda x: x[follow_idx]
        # follow samples
        follow_links = ifilter(filter_func, group_stream_cache.get_stream())
        follow_samples = self._compute_sample_links(
            follow_links, self.nb_samples)

        # nofollow samples
        follow_links = ifilterfalse(filter_func, group_stream_cache.get_stream())
        nofollow_samples = self._compute_sample_links(
            follow_links, self.nb_samples)

        return follow_samples, nofollow_samples

    def merge(self, domain, group_stream_cache):
        """Aggregate a group of pre-aggregated links info

        :param domain: the domain
        :type domain: str
        :param group_stream_cache: cached stream of a link group
        :type group_stream_cache: BufferedStreamCache
        """
        stats = self._compute_link_counts(
            domain, group_stream_cache.get_stream())
        nb_follow_unique = stats.follow_unique
        nb_nofollow_unique = stats.nofollow_unique
        if nb_follow_unique + nb_nofollow_unique == 0:
            # we don't want to return domain with 0 occurrences
            return

        insert_heap = False
        if len(self.heap) < self.n:
            insert_heap = True
        else:
            min_value = self.heap[0][0]
            if nb_follow_unique > min_value:
                insert_heap = True

        if insert_heap:
            sample_follow, sample_nofollow = self._compute_sample_sets(
                group_stream_cache,
            )
            stats.sample_follow_links = sample_follow
            stats.sample_nofollow_links = sample_nofollow
            if len(self.heap) < self.n:
                heapq.heappush(self.heap, (nb_follow_unique, stats))
            else:
                heapq.heappushpop(self.heap, (nb_follow_unique, stats))

    def get_result(self):
        """Return the result of the aggregator

        :return: list of stats per domain (DomainLinkStats)
        :rtype: list
        """
        result = []
        while len(self.heap) != 0:
            nb_follow_unique, domain_stats = heapq.heappop(self.heap)
            result.append(domain_stats)
        # sort by decreasing number of links
        result.reverse()
        return result


class TopSecondLevelDomainAggregator(TopDomainAggregator):
    """Aggregator for top second level domain analysis"""
    pass


class TopLevelDomainAggregator(TopDomainAggregator):
    """Aggregator for top level domain analysis"""

    @classmethod
    def top_level_domain_stream(cls, stream):
        for _, url, src, follow, count in stream:
            tld = get_domain(url)
            yield tld, url, src, follow, count

    def merge(self, domain, group_stream_cache):
        """For top-level domain, the top-level domain need to be extracted and
        served as the sort key

        :param domain: the domain
        :type domain: str
        :param group_stream_cache: cached stream of a link group
        :type group_stream_cache: BufferedStreamCache
        """
        stream = self.top_level_domain_stream(group_stream_cache.get_stream())
        # sort on (top_level_domain, url)
        # TODO maybe this sort is not necessary
        sorted_stream = external_sort(stream, key=lambda x: (x[0], x[1]))
        for tld, group in groupby(sorted_stream, lambda x: x[0]):
            group_cache = _get_stream_cache(group)
            super(TopLevelDomainAggregator, self).merge(tld, group_cache)


def compute_top_domain(external_outlinks, n, tmp_dir):
    """Compute `top_domain` analysis

    :param external_outlinks: filtered external outlinks,
        of format OutlinksRawStreamDef
    :type external_outlinks: iterator
    :param n: number of top domains to keep
    :type n: int
    :param tmp_dir: working directory
    :type tmp_dir: str
    :return: results of top-level domain and top-second-level domain analysis
    :rtype: tuple
    """
    top_level_domain = TopLevelDomainAggregator(n)
    top_second_level_domain = TopSecondLevelDomainAggregator(n)

    pre_aggregated = _pre_aggregate_link_stream(external_outlinks)
    db_stream = _encode_leveldb_stream(pre_aggregated)

    # TODO how to handle configs ???
    _LEVELDB_WRITE_BUFFER = 256 * 1024 * 1024  # 256M
    _LEVELDB_BLOCK_SIZE = 256 * 1024  # 256K
    _BUFFER_SIZE = 10000

    db = LevelDB(path=os.path.join(tmp_dir, 'linksdb'))
    db.open(write_buffer_size=_LEVELDB_WRITE_BUFFER,
            block_size=_LEVELDB_BLOCK_SIZE)

    with will_destroy(db) as db:
        db.batch_write(db_stream, batch_size=50000)

        decoded = _decode_leveldb_stream(db)

        for domain, link_group in groupby(decoded, lambda x: x[0]):
            logger.debug("Processing {} ...".format(domain))
            group_cache = _get_stream_cache(link_group)

            top_level_domain.merge(domain, group_cache)
            top_second_level_domain.merge(domain, group_cache)

    return top_level_domain.get_result(), top_second_level_domain.get_result()


def resolve_sample_url_id(urlids_stream, results):
    """Resolve and in-place replace url_id in the samples

    :param urlids_stream: the urlids stream
    :type urlids_stream: iterable
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

    id_to_url = get_id_to_url_dict_from_stream(urlids_stream, url_ids)

    for domain_stats in results:
        domain_stats.replace_ids(id_to_url)

    return results
