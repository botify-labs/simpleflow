from cdf.features.semantic_metadata.settings import CONTENT_TYPE_INDEX
from cdf.core.streams.base import StreamBase


def _str_to_bool(string):
    return string == '1'


class ContentsStream(StreamBase):
    FILE = 'urlcontents'
    HEADERS = (
        ('id', int),
        ('content_type', int),
        ('hash', int),
        ('txt', str)
    )

    def process_document(self, document, stream):
        content_type_id = stream[self.field_idx('content_type')]
        content_type = CONTENT_TYPE_INDEX[content_type_id]
        content = stream[self.field_idx('txt')]
        document['metadata'][content_type]['contents'].append(content)


class ContentsDuplicateStream(StreamBase):
    FILE = 'urlcontentsduplicate'
    HEADERS = (
        ('id', int),
        ('content_type', int),
        ('filled_nb', int),
        ('duplicates_nb', int),
        ('is_first_url', _str_to_bool),
        ('duplicate_urls', lambda k: [int(i) for i in k.split(';')] if k else [])
    )

    def process_document(self, document, stream):
        _, metadata_idx, nb_filled, nb_duplicates, is_first, duplicate_urls = stream
        metadata_type = CONTENT_TYPE_INDEX[metadata_idx]

        meta = document['metadata'][metadata_type]
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
        dup['is_first'] = is_first
