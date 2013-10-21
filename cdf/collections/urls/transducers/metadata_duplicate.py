import itertools

from collections import defaultdict

from cdf.streams.mapping import CONTENT_TYPE_INDEX, MANDATORY_CONTENT_TYPES_IDS
from cdf.streams.utils import group_left, idx_from_stream


def get_duplicate_metadata(stream_patterns, stream_contents):
    """
    Return a tuple of urls having a duplicate metadata (the first one found for each page)
    The 1st index is the url_id concerned
    The 2nd index is the content type (h1, title, description)
    The 3rd index is the number of filled anchors
    The 4th is the number of occurrences found for the first anchor for the whole crawl
    The 5th is a boolean that check if it is the first occurrence found in the whole crawl
    The 6th index is a list of the ten first url_ids found containg the same content type)

    H2 and H3 metadata are not concerned by 5 and 6

    (url_id, content_type, filled_nb, duplicates_nb, is_first_url_found, [url_id_1, url_id2 ...])
    """
    content_meta_type_idx = idx_from_stream('contents', 'content_type')
    content_hash_idx = idx_from_stream('contents', 'hash')
    streams_def = {
        'contents': (stream_contents, idx_from_stream('contents', 'id')),
    }
    hashes = defaultdict(lambda: defaultdict(set))

    # Resolve an url_id + ct_id to an hash : url_to_hash[url_id][ct_id] = hash_id
    url_to_hash = defaultdict(lambda: defaultdict(set))

    # Resolve the number of filled metadata for an given url : url_to_nb_filled[url_id][ct_id] = nb
    url_to_nb_filled = defaultdict(lambda: defaultdict(lambda: 0))

    for i, result in enumerate(group_left((stream_patterns, 0), **streams_def)):
        url_id = result[0]
        contents = result[2]['contents']

        if i == 0:
            min_url_id = url_id

        # Fetch --first-- hash from each content type and watch add it to hashes set
        ct_found = set()
        for content in contents:
            ct_id = content[content_meta_type_idx]
            url_to_nb_filled[url_id][ct_id] += 1
            if ct_id not in MANDATORY_CONTENT_TYPES_IDS:
                continue
            # If ct_i is already in ct_found, so it's the not the first content
            if ct_id not in ct_found:
                ct_found.add(ct_id)
                hashes[ct_id][content[content_hash_idx]].add(url_id)
                url_to_hash[url_id][ct_id] = content[content_hash_idx]

    # Take the last url_id
    max_url_id = url_id

    for url_id in xrange(min_url_id, max_url_id + 1):
        if url_id in url_to_hash:
            for ct_id in url_to_hash[url_id]:
                _h = url_to_hash[url_id][ct_id]
                if ct_id not in MANDATORY_CONTENT_TYPES_IDS:
                    yield (url_id, ct_id, url_to_nb_filled[url_id][ct_id], 0, True, [])
                urls = hashes[ct_id][_h]
                sample = list(itertools.islice(urls, 0, 11))
                nb_duplicates = len(urls)
                first_url_id = min(urls)
                yield (url_id, ct_id, url_to_nb_filled[url_id][ct_id], nb_duplicates, first_url_id == url_id, [i for i in sample if i != url_id][:10])
