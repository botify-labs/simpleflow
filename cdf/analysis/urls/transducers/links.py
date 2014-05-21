from collections import defaultdict
from itertools import groupby
from cdf.analysis.urls.utils import is_link_internal
from cdf.features.links.helpers.masks import is_first_canonical
from cdf.features.links.streams import InlinksRawStreamDef, OutlinksRawStreamDef


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
        url_id_idx = OutlinksRawStreamDef.field_idx('id')

        # Group the stream by url_id using itertools.groupby
        for url_id, group in groupby(self.stream_links, lambda x: x[url_id_idx]):
            counter_by_type = defaultdict(list)
            canonical = None
            for url, link_type, bitmask, dst_url_id, dst_url in group:

                # If the link is external and the follow_key is robots,
                # that means that the url is finally internal (not linked once in follow)
                # is_internal = dst_url_id > 0 or (dst_url_id == -1 and bitmask & 4 == 4)
                is_internal = is_link_internal(bitmask, dst_url_id, is_bitmask=True)

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
    """This transducer aggregates counters for incoming links

    For different link_type:
        - `a` link: number of incoming links by (url_id, link_type, bitmask)
        - redirection link: (url_id, 'redirect', nb_incoming_redirects)
        - canonical link: (url_id, 'canonical', nb_incoming_canonicals)
    It accepts a stream of INLINKS_RAW type
    """

    def __init__(self, stream_inlinks):
        self.stream_inlinks = stream_inlinks

    def get(self):
        url_id_idx = InlinksRawStreamDef.field_idx('id')

        # Group the stream by url_id using itertools.groupby
        for url_id, group in groupby(self.stream_inlinks, lambda x: x[url_id_idx]):
            counter_by_type = defaultdict(list)
            redirects = 0
            canonicals = set()
            for url, link_type, bitmask, src_url_id, txt_hash, txt in group:
                if link_type == 'a':
                    counter_by_type[bitmask].append(src_url_id)
                elif link_type.startswith('r'):
                    redirects += 1
                elif link_type == "canonical":
                    # Ignore identical canonicals
                    if url_id is src_url_id:
                        continue
                    # Ignore non-first canonical
                    if not is_first_canonical(bitmask):
                        continue
                    canonicals.add(src_url_id)

            for key, srcs in counter_by_type.iteritems():
                yield (url_id, 'links', key) + (len(srcs), len(set(srcs)))

            if redirects > 0:
                yield (url_id, 'redirect', redirects)

            if len(canonicals) > 0:
                yield (url_id, 'canonical', len(canonicals))
