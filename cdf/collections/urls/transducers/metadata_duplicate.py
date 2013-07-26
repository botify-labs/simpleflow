import itertools

from collections import defaultdict

from cdf.streams.mapping import CONTENT_TYPE_INDEX, MANDATORY_CONTENT_TYPES_IDS
from cdf.streams.utils import group_left, idx_from_stream


def get_duplicate_metadata(stream_patterns, stream_contents):
    """
    Return a tuple of urls having a duplicate metadata (the first one found for each page)
    The first index is the content type (h1, title, description)
    The 2nd index is the url_id concerned
    The 3rd index is a list of the ten first url_ids found containg the same content type)

    (content_type, url_id, [url_id_1, url_id2 ...])
    """
    content_meta_type_idx = idx_from_stream('contents', 'content_type')
    content_hash_idx = idx_from_stream('contents', 'hash')
    streams_def = {
        'contents': (stream_contents, idx_from_stream('contents', 'id')),
    }
    hashes = defaultdict(lambda: defaultdict(set))

    for result in group_left((stream_patterns, 0), **streams_def):
        url_id = result[0]
        contents = result[2]['contents']

        # Fetch --first-- hash from each content type and watch add it to hashes set
        ct_found = set()
        for content in contents:
            ct_id = content[content_meta_type_idx]
            if ct_id not in MANDATORY_CONTENT_TYPES_IDS:
                continue
            # If ct_i is already in ct_found, so it's the not the first content
            if ct_id not in ct_found:
                ct_found.add(ct_id)
                hashes[ct_id][content[content_hash_idx]].add(url_id)

    for ct_id, ct_hashes in hashes.iteritems():
        for _h in ct_hashes:
            if len(ct_hashes[_h]) > 1:
                # In case the set is too big, just iter on the first items
                sample = itertools.islice(ct_hashes[_h], 0, 11)
                for url_id in ct_hashes[_h]:
                    yield (CONTENT_TYPE_INDEX[ct_id], url_id, [i for i in sample if i != url_id][:10])
