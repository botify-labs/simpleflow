from cdf.metadata.url.url_metadata import (
    LONG_TYPE, INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    DATE_TYPE, ES_NOT_ANALYZED, ES_DOC_VALUE,
    LIST, AGG_CATEGORICAL, AGG_NUMERICAL, URL_ID
)
from cdf.core.streams.exceptions import GroupWithSkipException
from cdf.core.streams.base import StreamDefBase
from cdf.utils.date import date_2k_mn_to_date
from cdf.utils.hashing import string_to_int64
from cdf.query.constants import FIELD_RIGHTS, RENDERING

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
    URL_DOCUMENT_DEFAULT_GROUP = "scheme"
    URL_DOCUMENT_MAPPING = {
        # url property data
        "url": {
            "verbose_name": "Url",
            "type": STRING_TYPE,
            "order": 0,
            "settings": {
                ES_NOT_ANALYZED,
                RENDERING.URL
            }
        },
        "url_hash": {
            "verbose_name": "Url Hash",
            "type": LONG_TYPE,
            "settings": {
                FIELD_RIGHTS.PRIVATE,
                FIELD_RIGHTS.SELECT,
                FIELD_RIGHTS.FILTERS
            }
        },
        "host": {
            "verbose_name": "Host",
            "type": STRING_TYPE,
            "order": 1,
            "settings": {
                ES_NOT_ANALYZED,
                ES_DOC_VALUE,
                AGG_CATEGORICAL
            }
        },
        "id": {
            "verbose_name": "Id",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                FIELD_RIGHTS.PRIVATE,
                FIELD_RIGHTS.SELECT,
                FIELD_RIGHTS.FILTERS,
                URL_ID
            }
        },
        "crawl_id": {
            "type": INT_TYPE,
            "settings": {
                FIELD_RIGHTS.PRIVATE,
                FIELD_RIGHTS.SELECT,
                FIELD_RIGHTS.FILTERS
            }
        },
        "path": {
            "verbose_name": "Path",
            "type": STRING_TYPE,
            "order": 2,
            "settings": {ES_NOT_ANALYZED}
        },
        "protocol": {
            "verbose_name": "Protocol",
            "order": 3,
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED,
                ES_DOC_VALUE,
                AGG_CATEGORICAL
            }
        },
        "query_string": {
            "verbose_name": "Query String",
            "order": 4,
            "type": STRING_TYPE,
            "settings": {ES_NOT_ANALYZED}
        },
        "query_string_keys": {
            "verbose_name": "Query String Keys",
            "order": 5,
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
    URL_DOCUMENT_DEFAULT_GROUP = "main"
    URL_DOCUMENT_MAPPING = {
        "byte_size": {
            "verbose_name": "Byte Size",
            "type": INT_TYPE,
            "order": 0,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL
            }
        },
        "http_code": {
            "verbose_name": "Http Code",
            "type": INT_TYPE,
            "order": 1,
            "settings": {
                ES_DOC_VALUE,
                # `http_code` have 2 roles
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        },
        "date_crawled": {
            "verbose_name": "Date crawled",
            "type": DATE_TYPE,
            "order": 2,
            "settings": {ES_DOC_VALUE}
        },
        "delay_first_byte": {
            "verbose_name": "Delay first byte received",
            "type": INT_TYPE,
            "order": 3,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                RENDERING.TIME_MILLISEC
            }
        },
        "delay_last_byte": {
            "verbose_name": "Delay total",
            "type": INT_TYPE,
            "order": 4,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                RENDERING.TIME_MILLISEC
            }
        },
        "depth": {
            "verbose_name": "Depth",
            "type": INT_TYPE,
            "order": 5,
            "settings": {
                ES_DOC_VALUE,
                # assume possible depth is finite
                AGG_CATEGORICAL,
                AGG_NUMERICAL
            }
        },
        "gzipped": {
            "verbose_name": "Url compressed",
            "type": BOOLEAN_TYPE,
            "order": 7,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL
            }
        },
        "content_type": {
            "verbose_name": "Content-type",
            "type": STRING_TYPE,
            "order": 8,
            "settings": {
                ES_NOT_ANALYZED,
                ES_DOC_VALUE,
                AGG_CATEGORICAL
            }
        },
        # meta tag related
        "metadata.robots.nofollow": {
            "verbose_name": "Has robots anchors as `nofollow`",
            "type": BOOLEAN_TYPE,
            "order": 9,
            "settings": {AGG_CATEGORICAL}
        },
        "metadata.robots.noindex": {
            "verbose_name": "Has robots anchors as `noindex`",
            "type": BOOLEAN_TYPE,
            "order": 10,
            "settings": {AGG_CATEGORICAL}
        },
        "lang": {
            "verbose_name": "Lang",
            "type": STRING_TYPE,
            "order": 6,
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
            "verbose_name": "Suggested patterns",
            "type": LONG_TYPE,
            "settings": {
                LIST
            }
        },
    }

    def process_document(self, document, stream):
        url_id, pattern_hash = stream
        document['patterns'].append(pattern_hash)


class ZoneStreamDef(StreamDefBase):
    FILE = 'zones'
    HEADERS = (
        ('id', int),
        ('zone', str)
    )
    URL_DOCUMENT_MAPPING = {
        "zone": {
            "verbose_name": "Zone",
            "type": STRING_TYPE,
            "settings": {
                ES_NOT_ANALYZED
            }
        }
    }

    def process_document(self, document, stream):
        _, zone = stream
        document["zone"] = zone


def cast_bool(str):
    return str.lower() == 'true'


# TODO add `strategic_reason` field
class StrategicUrlStreamDef(StreamDefBase):
    FILE = 'strategic_urls'
    HEADERS = (
        ('id', int),
        ('strategic', cast_bool)
    )
    URL_DOCUMENT_MAPPING = {
        "strategic": {
            "verbose_name": "Strategic url",
            "type": BOOLEAN_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL
            }
        }
    }

    def process_document(self, document, stream):
        _, strategic = stream
        document['strategic'] = strategic
