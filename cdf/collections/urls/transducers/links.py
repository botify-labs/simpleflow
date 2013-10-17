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
                        yield (url_id, ) + key + (counter, )
                counter_by_type = defaultdict(lambda: 0)
                current_url_id = url_id
            is_internal = url_id > 0
            counter_by_type[(link_type, bitmask, 1 if is_internal else 0)] += 1


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
            url_id, link_type, bitmask, dst_url_id = entry
            if url_id != current_url_id:
                if current_url_id:
                    for key, counter in counter_by_type.iteritems():
                        yield (url_id, ) + key + (counter, )
                counter_by_type = defaultdict(lambda: 0)
                current_url_id = url_id
            counter_by_type[(link_type, bitmask)] += 1
