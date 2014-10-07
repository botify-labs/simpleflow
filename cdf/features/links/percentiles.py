from itertools import ifilter

from cdf.core.streams.utils import group_left
from cdf.features.main.streams import IdStreamDef
from cdf.features.links.helpers.predicates import is_follow_link
from cdf.features.links.streams import InlinksCountersStreamDef


class PercentileStats(object):
    """Monoid like class to represent stats of a percentile group
    """
    def __init__(self, metric_total, url_total, percentile_id, min, max):
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
    def new_emtpy(cls, pid):
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
        return {
            'id': self.percentile_id,
            'metric_total': self.metric_total,
            'url_total': self.url_total,
            'min': self.min,
            'max': self.max,
            'avg': self.avg
        }


def generate_follow_inlinks_stream(urlid_stream,
                                   inlinks_counter_stream,
                                   max_crawled_urlid):
    """Compute a stream of follow inlinks count.
    This function transforms InlinksCountersStreamDef which does not exactly
    fit our needs.
    It removes nofollow urls and insert elements for all crawled urlids.
    It generates a stream (nb follow links, urlid)
    :param urlid: the stream of urlids (based on IdStreamDef)
    :type urlid: iterator
    :param inlinks_counter_stream: the input stream
                                   (based on InlinksCountersStreamDef)
                                   that counts the number of inlinks per url.
    :type inlinks_counter_stream: iterator
    :param max_crawled_urlid: the highest urlid corresponding to a crawled url.
    :type max_crawled_urlid: int
    :param nb_quantiles: the number of quantiles (i.e. 100 for quantiles)
    :type nb_quantiles: int
    """
    follow_mask_index = InlinksCountersStreamDef.field_idx("follow")

    #keep only follow links
    inlinks_counter_stream = ifilter(
        lambda x: is_follow_link(x[follow_mask_index], is_bitmask=True),
        inlinks_counter_stream
    )
    urlid_id_index = IdStreamDef.field_idx("id")
    urlid_links_index = InlinksCountersStreamDef.field_idx("id")

    grouped_stream = group_left(
        left=(urlid_stream, urlid_id_index),
        inlinks_counter_stream=(inlinks_counter_stream, urlid_links_index))

    nb_links_index = InlinksCountersStreamDef.field_idx("score")
    for grouped_entries in grouped_stream:
        urlid = grouped_entries[0]
        counter = grouped_entries[2]["inlinks_counter_stream"]

        if urlid > max_crawled_urlid:
            break

        if len(counter) > 0:
            nb_links = counter[0][nb_links_index]
        else:
            #if the list is empty, there is no link
            nb_links = 0
        yield (urlid, nb_links)

