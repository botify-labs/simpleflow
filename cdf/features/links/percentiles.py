from itertools import ifilter

from cdf.utils.stream import split_stream
from cdf.core.streams.utils import group_left
from cdf.features.main.streams import InfosStreamDef
from cdf.features.links.helpers.predicates import is_follow_link
from cdf.features.links.streams import (
    InlinksCountersStreamDef,
    InredirectCountersStreamDef
)


class PercentileStats(object):
    """Monoid like class to represent stats of a percentile group
    """
    def __init__(self, metric_total, url_total, percentile_id, min, max):
        """Init a percentile stat instance

        Use the factory method `new_empty` instead of this ctor

        :param metric_total: total sum of all metrics
        :type metric_total: int
        :param url_total: total url count
        :type url_total: int
        :param percentile_id: percentile id
        :type percentile_id: int
        :param min: min value for the metric
        :type min: int
        :param max: max value for the metric
        :type max: int
        """
        self.metric_total = metric_total
        self.url_total = url_total
        self._min = min
        self._max = max
        self.percentile_id = percentile_id

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return (
            self.percentile_id == other.percentile_id and
            self.url_total == other.url_total and
            self.metric_total == other.metric_total and
            self.min == other.min and
            self.max == other.max
        )

    @property
    def avg(self):
        if self.url_total == 0:
            # simply returns 0 for empty percentile group
            return 0
        return self.metric_total / self.url_total

    @property
    def min(self):
        if self._min is None:
            return 0
        return self._min

    @property
    def max(self):
        if self._max is None:
            return 0
        return self._max

    @classmethod
    def new_empty(cls, pid):
        """Factory method for generating an empty percentile stats instance
        """
        return cls(0, 0, percentile_id=pid, min=None, max=None)

    def _merge_min(self, _min):
        if self._min is None:
            self._min = _min
        else:
            if _min < self._min:
                self._min = _min

    def _merge_max(self, _max):
        if self._max is None:
            self._max = _max
        else:
            if _max > self._max:
                self._max = _max

    def merge(self, metric_value):
        """Merge a metric value into the percentile group

        :param metric_value: domain metric value
        """
        self._merge_max(metric_value)
        self._merge_min(metric_value)
        self.metric_total += metric_value
        self.url_total += 1

    def to_dict(self):
        """Serialize this percentile stat in dict/json
        """
        return {
            'id': self.percentile_id,
            'metric_total': self.metric_total,
            'url_total': self.url_total,
            'min': self.min,
            'max': self.max,
            'avg': self.avg
        }


def generate_follow_inlinks_stream(urlinfo_stream,
                                   inlinks_counter_stream,
                                   inredirections_counter_stream,
                                   max_crawled_urlid):
    """Compute a stream of follow inlinks count - including the in redirections.
    This function transforms InlinksCountersStreamDef which does not exactly
    fit our needs.
    It removes nofollow urls and insert elements for all crawled urlids.
    It generates a stream (urlid, nb follow links)

    :param urlinfo_stream: the stream of urlinfo (based on InfosStreamDef)
    :type urlinfo_stream: iterator
    :param inlinks_counter_stream: the input stream
                                   (based on InlinksCountersStreamDef)
                                   that counts the number of inlinks per url.
    :type inlinks_counter_stream: iterator
    :param inredirections_counter_stream: the stream of in redirections counter
                                         (based on InredirectCountersStreamDef)
    :type inredirections_counter_stream: iterator
    :param max_crawled_urlid: the highest urlid corresponding to a crawled url.
    :type max_crawled_urlid: int
    :param nb_quantiles: the number of quantiles (i.e. 100 for quantiles)
    :type nb_quantiles: int
    """
    follow_mask_index = InlinksCountersStreamDef.field_idx("follow")
    http_code_index = InfosStreamDef.field_idx("http_code")

    #only follow links
    inlinks_counter_stream = ifilter(
        lambda x: is_follow_link(x[follow_mask_index], is_bitmask=False),
        inlinks_counter_stream
    )

    # only crawled urls
    urlinfo_stream = ifilter(
        lambda x: x[http_code_index] != 0,
        urlinfo_stream
    )

    urlid_info_index = InfosStreamDef.field_idx("id")
    urlid_links_index = InlinksCountersStreamDef.field_idx("id")
    urlid_redirections_index = InredirectCountersStreamDef.field_idx("id")

    grouped_stream = group_left(
        left=(urlinfo_stream, urlid_info_index),
        inlinks_counter_stream=(inlinks_counter_stream, urlid_links_index),
        inredirections_counter_stream=(inredirections_counter_stream, urlid_redirections_index)
    )

    nb_links_index = InlinksCountersStreamDef.field_idx("count_unique")
    nb_redirections_index = InredirectCountersStreamDef.field_idx("score")
    for grouped_entries in grouped_stream:
        urlid = grouped_entries[0]
        links_counter = grouped_entries[2]["inlinks_counter_stream"]
        redirections_counter = grouped_entries[2]["inredirections_counter_stream"]

        if urlid > max_crawled_urlid:
            break
        nb_links = 0
        for elt in links_counter:
            nb_links += elt[nb_links_index]
        for elt in redirections_counter:
            nb_links += elt[nb_redirections_index]

        yield (urlid, nb_links)


def compute_quantiles(urlid_stream,
                      inlinks_counter_stream,
                      inredirections_counter_stream,
                      max_crawled_urlid,
                      nb_quantiles):
    """Given a InlinksCountersStreamDef compute the quantile id of each url.
    The criterion used to determine the quantile id is
    the number of follow inlinks - including the in redirections.
    Basically the function sort the url by increasing number of follow inlinks
    and split the resulting stream in nb_quantiles chunks.
    Then it sort the result stream by urlid.
    The result stream has the form (url_id, percentile_id, nb_follow_inlinks)
    :param urlid: the stream of urlids (based on IdStreamDef)
    :type urlid: iterator
    :param inlinks_counter_stream: the input stream (based on InlinksCountersStreamDef)
                         that counts the number of inlinks per url.
    :type inlinks_counter_stream: iterator
    :param inredirections_counter_stream: the stream of in redirections counter
                                         (based on InredirectCountersStreamDef)
    :type inredirections_counter_stream: iterator
    :param max_crawled_urlid: the highest urlid corresponding to a crawled url.
    :type max_crawled_urlid: int
    :param nb_quantiles: the number of quantiles (i.e. 100 for quantiles)
    :type nb_quantiles: int
    """
    inlink_count_stream = generate_follow_inlinks_stream(
        urlid_stream,
        inlinks_counter_stream,
        inredirections_counter_stream,
        max_crawled_urlid
    )

    #the urls are sorted by increasing number of links and then
    #by decreasing urlid.
    #The second sort criterion is here only to ensure that the process is
    #deterministic.
    #High urlids tend to have high quantile ids because they are high level urls
    #To respect this, we sort by decreasing urlid.
    inlink_count = sorted(list(inlink_count_stream), key=lambda x: (x[1], -x[0]))
    #generate quantile stream
    result = []
    quantile_generator = split_stream(inlink_count, len(inlink_count), nb_quantiles)
    for quantile_index, urlids in enumerate(quantile_generator):
        # quantile starts from 1
        quantile_index += 1
        for urlid, nb_follow_inlinks in urlids:
            result.append((urlid, quantile_index, nb_follow_inlinks))

    #sort stream by urlid
    #as the amount of data per url is small, we know that the data fits in
    #memory and we can use "sorted"
    for element in sorted(result, key=lambda x: x[0]):
        yield element


def compute_percentile_stats(percentile_stream):
    """Compute percentile stats by a pass of percentile stream

    :param percentile_stream: percentile stream of form:
        (`url_id`, `percentile_id`, `metric_value`)
    :type percentile_stream: iterator
    :return: list of stats of each percentile
    :rtype: list
    """
    stats = {}
    for url_id, pid, metric_value in percentile_stream:
        index = pid - 1
        if index not in stats:
            # percentile_id starts from 1
            stats[index] = PercentileStats.new_empty(pid)
        stat = stats[index]
        stat.merge(metric_value)

    # in-memory operations since at most 100 percentiles
    return [stats[i] for i in sorted(stats)]
