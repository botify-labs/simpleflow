from itertools import ifilter

from cdf.core.streams.utils import group_left
from cdf.features.main.streams import IdStreamDef
from cdf.features.links.helpers.predicates import is_follow_link
from cdf.features.links.streams import InlinksCountersStreamDef


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

