from collections import defaultdict
from itertools import groupby
from cdf.streams.utils import idx_from_stream


class OutlinksTransducer(object):
    """This transducers aggregates counters for outgoing links

    For different link_type:
        - `a` link: returns the number of links by (url_id, link_type, bitmask and internal status)
        - redirection link: (url_id, 'redirect', is_internal)
        - canonical link: (url_id, 'canonical', is_equals)
    It accepts a stream of OUTLINKS_RAW type
    """

    def __init__(self, stream_links):
        self.stream_links = stream_links

    def get(self):
        url_id_idx = idx_from_stream('outlinks_raw', 'id')

        # Group the stream by url_id using itertools.groupby
        for url_id, group in groupby(self.stream_links, lambda x: x[url_id_idx]):
            counter_by_type = defaultdict(list)
            canonical = None
            for url, link_type, bitmask, dst_url_id, dst_url in group:

                # If the link is external and the follow_key is robots,
                # that means that the url is finally internal (not linked once in follow)
                is_internal = dst_url_id > 0 or (dst_url_id == -1 and bitmask & 4 == 4)

                if link_type == 'a':
                    counter_by_type[(bitmask, 1 if is_internal else 0)].append(
                        dst_url_id if dst_url_id > 0 else dst_url)
                elif link_type.startswith('r'):
                    yield (url_id, 'redirect', 1 if is_internal else 0)
                elif link_type == 'canonical':
                    # Take only first canonical into account
                    if canonical is None:
                        canonical = 1 if url_id == dst_url_id else 0
                        yield (url_id, 'canonical', canonical)

            for key, dsts in counter_by_type.iteritems():
                yield (url_id, 'links') + key + (len(dsts), len(set(dsts)))


class InlinksTransducer(object):
    """
    This transducers returns the number of links by url_id, link_type,  bitmask and internal status
    It accepts a stream of INLINKS_RAW type
    """

    def __init__(self, stream_links):
        self.stream_links = stream_links

    def get(self):
        current_url_id = None

        for entry in self.stream_links:
            url_id, link_type, bitmask, src_url_id = entry
            if url_id != current_url_id:
                if current_url_id:
                    for bitmask, urls in counter_by_type.iteritems():
                        # returns a tuple (url_id, 'links', bitmask, nb_incoming_links, nb_incoming_links_unique)
                        yield (url_id, 'links', bitmask, len(urls), len(set(urls)))
                    if len(canonicals) > 0:
                        # returns a tuple (url_id, 'canonical', nb_canonicals_incoming)
                        yield (url_id, 'canonical', len(canonicals))
                    if redirects > 0:
                        # returns a tuple (url_id, 'redirects', nb_redirects_incoming)
                        yield (url_id, 'redirect', redirects)
                counter_by_type = defaultdict(list)
                canonicals = set()
                redirects = 0
                current_url_id = url_id

            if link_type == "a":
                counter_by_type[bitmask].append(src_url_id)
            elif link_type == "canonical":
                if url_id != src_url_id:
                    canonicals.add(src_url_id)
            elif link_type.startswith('r'):
                redirects += 1
