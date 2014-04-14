from cdf.features.semnatic_data.settings import CONTENT_TYPE_INDEX, MANDATORY_CONTENT_TYPES
from cdf.core.streams.utils import idx_from_stream


def _content_types(mandatory=False):
    for content_name in CONTENT_TYPE_INDEX.itervalues():
        if not mandatory:
            yield content_name
        if mandatory and content_name in MANDATORY_CONTENT_TYPES:
            yield content_name


def _process_contents(doc, stream_contents):
    content_type_id = stream_contents[idx_from_stream('contents', 'content_type')]
    content_type = CONTENT_TYPE_INDEX[content_type_id]
    content = stream_contents[idx_from_stream('contents', 'txt')]
    doc['metadata'][content_type]['contents'].append(content)


def _process_metadata_duplicate(doc, stream_duplicate):
    _, metadata_idx, nb_filled, nb_duplicates, is_first, duplicate_urls = stream_duplicate
    metadata_type = CONTENT_TYPE_INDEX[metadata_idx]

    meta = doc['metadata'][metadata_type]
    # number of metadata of this kind
    meta['nb'] = nb_filled
    # number of duplications of this piece of metadata
    dup = meta['duplicates']
    dup['nb'] = nb_duplicates
    # urls that have duplicates
    if nb_duplicates > 0:
        dup['urls'] = duplicate_urls
        dup['urls_exists'] = True

    # is this the first one out of all duplicates


PROCESSORS = {
    'contents': _process_contents,
    'contents_duplicate': _process_metadata_duplicate,
}
