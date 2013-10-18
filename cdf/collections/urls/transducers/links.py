from collections import defaultdict


class OutlinksTransducer(object):
    """
    This transducers returns the number of links by url_id, link_type,  bitmask and internal status
    It accepts a stream of OUTLINKS_RAW type
    """

    def __init__(self, stream_links):
        self.stream_links = stream_links

    def get(self):
        current_url_id = None

        for entry in self.stream_links:
            url_id, link_type, bitmask, dst_url_id, dst_url = entry
            if url_id != current_url_id:
                if current_url_id:
                    for key, counter in counter_by_type.iteritems():
                        # Return a tupole (url_id, 'links', bitmask, is_internal, nb_links, nb_links_unique
                        yield (url_id, 'links') + key + (len(counter), len(set(counter)))
                    if redirect is not None:
                        # Return a tuple (url_id, 'redirect', is_internal)
                        yield (url_id, 'redirect', redirect)
                    if canonical is not None:
                        # Return a tuple (url_id, 'canonical', is_equals)
                        yield (url_id, 'canonical', canonical)
                counter_by_type = defaultdict(list)
                canonical = None
                redirect = None
                current_url_id = url_id
            is_internal = url_id > 0

            if link_type == "a":
                if is_internal:
                    counter_by_type[(bitmask, 1)].append(dst_url_id)
                else:
                    counter_by_type[(bitmask, 0)].append(dst_url)
            elif link_type == "canonical":
                # Fetch only the first canonical
                if canonical is None:
                    canonical = 1 if url_id == dst_url_id else 0
            elif link_type.startswith('r'):
                redirect = 1 if is_internal else 0


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
