from cdf.metadata.url.url_metadata import (
    LONG_TYPE, INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    DATE_TYPE, ES_NOT_ANALYZED, ES_DOC_VALUE,
    LIST, AGG_CATEGORICAL, AGG_NUMERICAL
)
from cdf.core.streams.exceptions import GroupWithSkipException
from cdf.core.streams.base import StreamDefBase
from cdf.utils.date import date_2k_mn_to_date
from cdf.utils.hashing import string_to_int64

__all__ = ["IdStreamDef", "InfosStreamDef", "SuggestStreamDef"]


class IdStreamDef(StreamDefBase):
    FILE = 'urlids'
    HEADERS = (
        ('id', int),
        ('protocol', str),
        ('host', str),
        ('path', str),
        ('query_string', str),
    )
    URL_DOCUMENT_DEFAULT_GROUP = "metrics"
    URL_DOCUMENT_MAPPING = {
        # url property data
        "url": {
            "verbose_name": "Url",
            "type": STRING_TYPE,
            "settings": {ES_NOT_ANALYZED}
        },
        "url_hash": {
            "verbose_name": "Url Hash",
            "type": LONG_TYPE
        },
        "host": {
            "verbose_name": "Host",
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_DOC_VALUE,
                AGG_CATEGORICAL
            }
        },
        "id": {
            "verbose_name": "Id",
            "type": INT_TYPE,
            "settings": {ES_DOC_VALUE}
        },
        "crawl_id": {"type": INT_TYPE},
        "path": {
            "verbose_name": "Path",
            "type": STRING_TYPE,
            "settings": {ES_NOT_ANALYZED}
        },
        "protocol": {
            "verbose_name": "Protocol",
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_DOC_VALUE,
                AGG_CATEGORICAL
            }
        },
        "query_string": {
            "verbose_name": "Query String",
            "type": STRING_TYPE,
            "settings": {ES_NOT_ANALYZED}
        },
        "query_string_keys": {
            "verbose_name": "Query String Keys",
            "type": STRING_TYPE,
            "settings": {ES_NOT_ANALYZED}
        },
    }

    def process_document(self, document, stream):
        """Init the document and process `urlids` stream
        """
        # simple information about each url
        document.update(self.to_dict(stream))
        document['url'] = document['protocol'] + '://' + ''.join(
            (document['host'], document['path'], document['query_string']))
        document['url_hash'] = string_to_int64(document['url'])

        query_string = stream[4]
        if query_string:
            # The first character is ? we flush it in the split
            qs = [k.split('=') if '=' in k else [k, '']
                  for k in query_string[1:].split('&')]
            document['query_string_keys'] = [q[0] for q in qs]


class InfosStreamDef(StreamDefBase):
    FILE = 'urlinfos'
    HEADERS = (
        ('id', int),
        ('infos_mask', int),
        ('content_type', str),
        ('depth', int),
        ('date_crawled', int),
        ('http_code', int),
        ('byte_size', int),
        ('delay_first_byte', int),
        ('delay_last_byte', int),
        ('lang', str, {"default": "notset", "missing": "notset"})
    )
    URL_DOCUMENT_MAPPING = {
        "byte_size": {
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL
            }
        },
        "http_code": {
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                # `http_code` have 2 roles
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        },
        "date_crawled": {
            "type": DATE_TYPE,
            "settings": {ES_DOC_VALUE}
        },
        "delay_first_byte": {
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL
            }
        },
        "delay_last_byte": {
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL
            }
        },
        "depth": {
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                # assume possible depth is finite
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        },
        "gzipped": {"type": BOOLEAN_TYPE},
        "content_type": {
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_DOC_VALUE,
                AGG_CATEGORICAL
            }
        },
        # meta tag related
        "metadata.robots.nofollow": {
            "type": BOOLEAN_TYPE,
            "settings": {AGG_CATEGORICAL}
        },
        "metadata.robots.noindex": {
            "type": BOOLEAN_TYPE,
            "settings": {AGG_CATEGORICAL}
        },
        "lang": {
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_DOC_VALUE,
                AGG_CATEGORICAL
            },
            "enabled": lambda options: options and options.get("lang", False)
        }
    }

    def process_document(self, document, stream):
        """Process `urlinfos` stream
        """
        date_crawled_idx = self.field_idx('date_crawled')
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

        # mask:
        # 1 gzipped, 2 notused, 4 meta_noindex
        # 8 meta_nofollow 16 has_canonical 32 bad canonical
        infos_mask = stream[self.field_idx('infos_mask')]
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
            if (
                ('redirect' in document and document['redirect']['from']['nb'] > 0) or
                ('canonical' in document and document['canonical']['from']['nb'] > 0)
            ):
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


class SuggestStreamDef(StreamDefBase):
    FILE = 'url_suggested_clusters'
    HEADERS = (
        ('id', int),
        ('query_hash', str)
    )
    URL_DOCUMENT_MAPPING = {
        "patterns": {
            "type": LONG_TYPE,
            "settings": {
                LIST
            }
        },
    }

    def process_document(self, document, stream):
        url_id, pattern_hash = stream
        document['patterns'].append(pattern_hash)
