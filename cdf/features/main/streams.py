from cdf.core.streams.exceptions import GroupWithSkipException
from cdf.core.streams.utils import idx_from_stream
from cdf.core.streams.base import StreamBase
from cdf.utils.date import date_2k_mn_to_date
from cdf.utils.hashing import string_to_int64

__all__ = ["PatternsStream", "InfosStream", "SuggestStream"]


class PatternsStream(StreamBase):
    FILE = 'urlids'
    HEADERS = (
        ('id', int),
        ('protocol', str),
        ('host', str),
        ('path', str),
        ('query_string', str),
    )

    def process_document(self, document, stream):
        """Init the document and process `urlids` stream
        """

        # simple information about each url
        document.update(self.extract_stream_fields(stream))
        document['url'] = document['protocol'] + '://' + ''.join(
            (document['host'], document['path'], document['query_string']))
        document['url_hash'] = string_to_int64(document['url'])

        query_string = stream[4]
        if query_string:
            # The first character is ? we flush it in the split
            qs = [k.split('=') if '=' in k else [k, '']
                  for k in query_string[1:].split('&')]
            document['query_string_keys'] = [q[0] for q in qs]


class InfosStream(StreamBase):
    FILE = 'urlinfos'
    HEADERS = (
        ('id', int),
        ('infos_mask', int),
        ('content_type', str),
        ('depth', int),
        ('date_crawled', int),
        ('http_code', int),
        ('byte_size', int),
        ('delay1', int),
        ('delay2', int),
    )

    def process_document(self, document, stream):
        """Process `urlinfos` stream
        """
        date_crawled_idx = idx_from_stream('infos', 'date_crawled')
        stream[date_crawled_idx] = date_2k_mn_to_date(
            stream[date_crawled_idx]).strftime("%Y-%m-%dT%H:%M:%S")
        # TODO could skip non-crawled url here
        # http code 0, 1, 2 are reserved for non-crawled urls

        document.update(self.to_dict(stream))
        # infos_mask has a special process
        del(document['infos_mask'])

        # `?` should be rename to `not-set`
        if document['content_type'] == '?':
            document['content_type'] = 'not-set'

        # rename `delay1` and `delay2`
        # they are kept in the stream schema for compatibility with
        document['delay_first_byte'] = document['delay1']
        document['delay_last_byte'] = document['delay2']
        del(document['delay1'])
        del(document['delay2'])

        # mask:
        # 1 gzipped, 2 notused, 4 meta_noindex
        # 8 meta_nofollow 16 has_canonical 32 bad canonical
        infos_mask = stream[idx_from_stream('infos', 'infos_mask')]
        document['gzipped'] = 1 & infos_mask == 1

        target = document['metadata']['robots']
        target['noindex'] = 4 & infos_mask == 4
        target['nofollow'] = 8 & infos_mask == 8

    def post_process_document(self, document):
        """Final process the whole generated document

        It does several things:
            - remove temporary attributes used by other processing
            - remove non-crawled url document unless it receives redirection
              or canonical links
            - some analytic processing that needs a global view of the whole
              document
            - control the size of some list (eg. list of links)
        """
        # include not crawled url in generated document only if they've received
        # redirection or canonicals
        if document['http_code'] in (0, 1, 2):
            redirect_from_nb = document['redirect']['from']['nb']
            canonical_from_nb = document['canonical']['from']['nb']
            if redirect_from_nb > 0 or canonical_from_nb > 0:
                url = document['url']
                url_id = document['id']
                document.clear()
                document.update({
                    'id': url_id,
                    'url': url,
                    'http_code': 0
                })
            else:
                raise GroupWithSkipException()


class SuggestStream(StreamBase):
    FILE = 'url_suggested_clusters'
    HEADERS = (
        ('id', int),
        ('query_hash', str)
    )

    def process_document(self, document, stream):
        url_id, pattern_hash = stream
        document['patterns'].append(pattern_hash)
