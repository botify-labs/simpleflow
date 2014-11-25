from collections import Counter
from itertools import groupby, ifilter, imap
from operator import itemgetter
from cdf.core.streams.utils import group_left
from cdf.features.main.streams import CompliantUrlStreamDef
from cdf.features.semantic_metadata.settings import MANDATORY_CONTENT_TYPES_IDS
from cdf.features.semantic_metadata.streams import (
    ContentsStreamDef,
    ContentsDuplicateStreamDef
)
from cdf.utils.external_sort import external_sort

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


def filter_redundant_metadata(stream_contents):
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


def generate_duplicate_stream(stream_contents, key):
    """Generate duplicate stream (based on ContentsDuplicateStreamDef)
    from a contents stream.
    :param stream_contents: the contents stream
                            (as we usually work the content hashes,
                            actual content may have been removed from the stream)
    :type stream_contents: iterator
    :param key: the key function to decide whether two entries are duplicates or not.
    :type key: fun
    :returns: iterator
    """
    url_id_idx = ContentsStreamDef.field_idx('id')
    content_meta_type_idx = ContentsStreamDef.field_idx('content_type')

    # only preserve 10 duplicating urls
    nb_samples_to_return = 10
    #use external sort to prevent swapping
    stream_contents = external_sort(stream_contents, key=key)
    for _, contents in groupby(stream_contents, key=key):
        #required to know the list lenght
        contents = list(contents)
        content_lenght = len(contents)
        url_ids = [content[url_id_idx] for content in contents]
        min_url_id = min(url_ids)  # to detect if an url id is the first occurrence.
        sample_candidates = url_ids[:nb_samples_to_return + 1]
        for content in contents:
            url_id = content[url_id_idx]
            ct_id = content[content_meta_type_idx]
            nb_duplicates = content_lenght
            # Unique (url, metatype)'s duplicates number should be 0, intuitively
            # Simple hack here, we should not push no-duplicate records to ES and
            # generates necessary information in document generator (like `filled_nb`)
            if nb_duplicates == 1:
                nb_duplicates = 0
            samples = [i for i in sample_candidates if i != url_id][:nb_samples_to_return]
            yield (url_id, ct_id, nb_duplicates, url_id == min_url_id, samples)


def preprocess_duplicate_computation(stream_contents):
    """Preprocess a contents stream so that it is ready for duplicate detection.
    Preprocessing includes steps like:
    - non mandatory content types removal
    - removal of the 2nd, 3rd titles
    - etc.
    :param stream_contents: the input content stream
                            (based on ContentsStreamDef)
    :type stream_contents: iterator
    :returns: iterator
    """
    # Resolve indexes
    url_id_idx = ContentsStreamDef.field_idx('id')
    content_meta_type_idx = ContentsStreamDef.field_idx('content_type')
    content_hash_idx = ContentsStreamDef.field_idx('hash')

    #ignore not mandatory content types
    stream_contents = ifilter(
        lambda x: x[content_meta_type_idx] in MANDATORY_CONTENT_TYPES_IDS,
        stream_contents
    )
    #ignore notset metadata, they don't count anything
    stream_contents = ifilter(
        lambda x: x[content_hash_idx] != notset_hash_value,
        stream_contents
    )
    stream_contents = filter_redundant_metadata(stream_contents)
    #remove actual content (to save memory)
    stream_contents = imap(
        itemgetter(url_id_idx, content_meta_type_idx, content_hash_idx),
        stream_contents
    )
    return stream_contents


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
    #stream preprocessing
    stream_contents = preprocess_duplicate_computation(stream_contents)

    #actual duplicate computation
    get_hash_and_content_type = itemgetter(2, 1)  # content hash, content type
    stream_duplicates = generate_duplicate_stream(
        stream_contents,
        key=get_hash_and_content_type
    )

    #the output stream is different from input stream
    #thus the index might be different
    url_id_idx = ContentsDuplicateStreamDef.field_idx('id')
    content_meta_type_idx = ContentsDuplicateStreamDef.field_idx('content_type')
    #sort by urlid
    stream_duplicates = external_sort(
        stream_duplicates,
        key=itemgetter(url_id_idx, content_meta_type_idx)
    )

    return stream_duplicates


def filter_non_compliant_urls(stream_contents,
                              stream_compliant_urls):
    """Remove non compliant urls from a contents stream.
    :param stream_contents: the input contents stream.
                            (based on ContentsStreamDef)
    :type stream_contents: iterator
    :param stream_compliant_urls: the input compliant_url stream
                                  (based on CompliantUrlStreamDef)
    :type stream_compliant_urls: iterator
    :returns: iterator
    """
    grouped_stream = group_left(
        (stream_compliant_urls, 0),
        contents=(stream_contents, 0)
    )
    #actual filtering
    compliant_idx = CompliantUrlStreamDef.field_idx("compliant")
    grouped_stream = ifilter(
        lambda x: x[1][compliant_idx],
        grouped_stream
    )
    grouped_stream = imap(lambda x: x[2]["contents"], grouped_stream)
    for elts in grouped_stream:
        for elt in elts:
            yield elt


def append_zone(stream_contents, stream_zones):
    """Append the zone to a contents stream
    :param stream_contents: the input contents stream.
                            (based on ContentsStreamDef)
    :type stream_contents: iterator
    :param stream_zone: the input zone stream
                        (based on ZoneStreamDef)
    :type stream_zone: iterator
    :returns: iterator
    """
    grouped_stream = group_left(
        (stream_zones, 0),
        contents=(stream_contents, 0)
    )
    for _, zone_elt, contents in grouped_stream:
        _, zone = zone_elt
        for elt in contents["contents"]:
            yield elt + (zone,)


def get_context_aware_duplicate_metadata(stream_contents,
                                         stream_zones,
                                         stream_compliant_urls):
    """
    Return a tuple of urls having a duplicate metadata (the first one found for each page).
    The difference with get_duplicate_metadata() is that:
    - it consider only compliant urls
    - two contents from different zones are always considered as different
    The 1st index is the url_id concerned
    The 2nd index is the content type (h1, title, description)
    The 3rd is the number of occurrences found for the first anchor for the whole crawl
    The 4th is a boolean that check if it is the first occurrence found in the whole crawl
    The 5th index is a list of the ten first url_ids found containg the same content type)

    H2 and H3 metadata are not concerned by 4 and 5

    (url_id, content_type, filled_nb, duplicates_nb, is_first_url_found, [url_id_1, url_id2 ...])
    """
    #stream preprocessing
    stream_contents = preprocess_duplicate_computation(stream_contents)

    #remove non compliant urls
    stream_contents = filter_non_compliant_urls(stream_contents,
                                                stream_compliant_urls)

    stream_contents = append_zone(stream_contents, stream_zones)
    #actual duplicate computation
    get_hash_and_content_type = itemgetter(2, 1, -1)  # content hash, content type, zone
    stream_duplicates = generate_duplicate_stream(
        stream_contents,
        key=get_hash_and_content_type
    )

    #the output stream is different from input stream
    #thus the index might be different
    url_id_idx = ContentsDuplicateStreamDef.field_idx('id')
    content_meta_type_idx = ContentsDuplicateStreamDef.field_idx('content_type')
    #sort by urlid
    stream_duplicates = external_sort(
        stream_duplicates,
        key=itemgetter(url_id_idx, content_meta_type_idx)
    )

    return stream_duplicates

