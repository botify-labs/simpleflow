from collections import defaultdict, Counter
from itertools import groupby, ifilter

from cdf.features.semantic_metadata.settings import MANDATORY_CONTENT_TYPES_IDS
from cdf.features.semantic_metadata.streams import ContentsStreamDef


# notset metadata is interpreted as an empty string
# they should be ignored by duplication detection
notset_hash_value = 14695981039346656037


def count_metadata(stream_contents, part_id):
    """Count the number of title, description, h1, h2, etc. for each urlid
    of a part.
    The function ignores the metadata which are not set.
    :param stream_contents: the input stream (based on ContentsStreamDef)
    :type stream_contents: iterator
    :param part_id: the input part_id
    :type part_id: int
    :returns: iterator - a stream (url_id, content_type_id, count)
    """
    # Resolve indexes
    url_id_idx = ContentsStreamDef.field_idx('id')
    content_meta_type_idx = ContentsStreamDef.field_idx('content_type')
    content_hash_idx = ContentsStreamDef.field_idx('hash')

    #ignore notset metadata, they don't count anything
    stream_contents = ifilter(lambda x: x[content_hash_idx] != notset_hash_value,
                              stream_contents)
    for url_id, contents in groupby(stream_contents, lambda x: x[url_id_idx]):
        filled_counter = Counter()
        for content in contents:
            ct_id = content[content_meta_type_idx]
            filled_counter[ct_id] += 1

        for ct_id, filled_nb in sorted(filled_counter.iteritems()):
            yield (url_id, ct_id, filled_nb)


def keep_only_first_metadata(stream_contents):
    """Given a contents stream, keep only the entries corresponding to the
    first title, first h1, etc.
    :param stream_contents: the input contents stream
                            (based on ContentsStreamDef)
    :type stream_contents: Stream
    """
    url_id_idx = ContentsStreamDef.field_idx('id')
    content_meta_type_idx = ContentsStreamDef.field_idx('content_type')
    for url_id, contents in groupby(stream_contents, lambda x: x[url_id_idx]):
        # Fetch --first-- hash from each content type and watch add it to hashes set
        ct_found = set()
        # they should be ignored by duplication detection
        for content in contents:

            ct_id = content[content_meta_type_idx]
            # If ct_i is already in ct_found, so it's the not the first content
            if ct_id not in ct_found:
                ct_found.add(ct_id)
                yield content


def get_duplicate_metadata(stream_contents):
    """
    Return a tuple of urls having a duplicate metadata (the first one found for each page)
    The 1st index is the url_id concerned
    The 2nd index is the content type (h1, title, description)
    The 3rd is the number of occurrences found for the first anchor for the whole crawl
    The 4th is a boolean that check if it is the first occurrence found in the whole crawl
    The 5th index is a list of the ten first url_ids found containg the same content type)

    H2 and H3 metadata are not concerned by 4 and 5

    (url_id, content_type, filled_nb, duplicates_nb, is_first_url_found, [url_id_1, url_id2 ...])
    """
    # Resolve indexes
    url_id_idx = ContentsStreamDef.field_idx('id')
    content_meta_type_idx = ContentsStreamDef.field_idx('content_type')
    content_hash_idx = ContentsStreamDef.field_idx('hash')

    hashes = defaultdict(lambda: defaultdict(list))
    hashes_count = defaultdict(Counter)

    # Resolve an url_id + ct_id to an hash : url_to_hash[url_id][ct_id] = hash_id
    url_to_hash = defaultdict(lambda: defaultdict(set))

    #ignore not mandatory content types
    stream_contents = ifilter(lambda x: x[content_meta_type_idx] in MANDATORY_CONTENT_TYPES_IDS, stream_contents)
    #ignore notset metadata, they don't count anything
    stream_contents = ifilter(lambda x: x[content_hash_idx] != notset_hash_value,
                              stream_contents)
    stream_contents = keep_only_first_metadata(stream_contents)

    # only preserve 10 duplicating urls
    nb_samples_to_return = 10
    for content in stream_contents:
        url_id = content[url_id_idx]
        ct_id = content[content_meta_type_idx]

        _hash = content[content_hash_idx]
        hashes_count[ct_id][_hash] += 1
        hashes_lst = hashes[ct_id][_hash]
        #+1 because we want to return nb_samples_to_return
        #which are different from the current url_id
        if len(hashes_lst) < nb_samples_to_return + 1:
            hashes[ct_id][_hash].append(url_id)
        url_to_hash[url_id][ct_id] = _hash

    min_url_id = min(url_to_hash.iterkeys())
    max_url_id = max(url_to_hash.iterkeys())
    for url_id in xrange(min_url_id, max_url_id + 1):
        if url_id not in url_to_hash:
            continue
        for ct_id, _h in url_to_hash[url_id].iteritems():
            urls = hashes[ct_id][_h]
            nb_duplicates = hashes_count[ct_id][_h]
            # Unique (url, metatype)'s duplicates number should be 0, intuitively
            # Simple hack here, we should not push no-duplicate records to ES and
            # generates necessary information in document generator (like `filled_nb`)
            if nb_duplicates == 1:
                nb_duplicates = 0

            # Since duplicating urls are appended to a list, order is preserved
            # The first url is garanteed to be the min
            # urls list has at least one elem. (url itself)
            first_url_id = urls[0]
            yield (url_id, ct_id, nb_duplicates,
                   first_url_id == url_id,
                   [i for i in urls if i != url_id][:nb_samples_to_return])

