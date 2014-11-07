# -*- coding: utf-8 -*-
from collections import Counter
from cdf.core.metadata.dataformat import check_enabled

from cdf.metadata.url.url_metadata import (
    INT_TYPE, BOOLEAN_TYPE, STRUCT_TYPE,
    ES_NO_INDEX, ES_DOC_VALUE,
    LIST, AGG_CATEGORICAL, AGG_NUMERICAL,
    STRING_TYPE, ES_NOT_ANALYZED, STRING_NB_MAP_MAPPING,
    FAKE_FIELD, URL_ID,
    DIFF_QUALITATIVE, DIFF_QUANTITATIVE
)
from cdf.core.streams.base import StreamDefBase
from cdf.log import logger
from cdf.features.links.helpers.masks import list_to_mask
from cdf.utils.convert import _raw_to_bool
from cdf.core.metadata.constants import RENDERING, FIELD_RIGHTS
from cdf.features.links.helpers.predicates import is_link_internal
from cdf.features.links.helpers.masks import follow_mask
from cdf.features.links.settings import GROUPS, NB_TOP_ANCHORS


__all__ = ["OutlinksRawStreamDef", "OutlinksStreamDef"]


def _get_nofollow_combination_key(keys):
    return '_'.join(sorted(keys))


class OutlinksRawStreamDef(StreamDefBase):
    FILE = 'urllinks'
    HEADERS = (
        ('id', int),
        ('link_type', str),
        ('bitmask', int),
        ('dst_url_id', int),
        ('external_url', str)
    )


class OutlinksStreamDef(OutlinksRawStreamDef):
    """
    We just change the type of "follow"
    """
    HEADERS = (
        ('id', int),
        ('link_type', str),
        ('follow', follow_mask),
        ('dst_url_id', int),
        ('external_url', str)
    )
    URL_DOCUMENT_MAPPING = {
        # internal outgoing links (destination is a internal url)
        "outlinks_internal.nb.total": {
            "verbose_name": "Number of Internal Outlinks",
            "group": GROUPS.outlinks_internal.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.unique": {
            "verbose_name": "Unique Number of Internal Outlinks",
            "group": GROUPS.outlinks_internal.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.follow.unique": {
            "verbose_name": "Unique Number of Follow Internal Outlinks",
            "group": GROUPS.outlinks_internal.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.follow.total": {
            "verbose_name": "Number of Internal Follow Outlinks",
            "group": GROUPS.outlinks_internal.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.nofollow.unique": {
            "verbose_name": "Unique Number of NoFollow Internal Outlinks",
            "group": GROUPS.outlinks_internal_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.nofollow.total": {
            "verbose_name": "Number of Internal NoFollow Outlinks",
            "group": GROUPS.outlinks_internal_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.nofollow.combinations.link": {
            "verbose_name": "Number of Internal NoFollow Outlinks strictly in link nofollow",
            "group": GROUPS.outlinks_internal_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.nofollow.combinations.meta": {
            "verbose_name": "Number of Internal NoFollow Outlinks strictly in meta nofollow",
            "group": GROUPS.outlinks_internal_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.nofollow.combinations.robots": {
            "verbose_name": "Number of Internal NoFollow Outlinks strictly disallowed by robots.txt",
            "group": GROUPS.outlinks_internal_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.nofollow.combinations.link_meta": {
            "verbose_name": "Number of Internal NoFollow Outlinks both in link and meta nofollow",
            "group": GROUPS.outlinks_internal_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.nofollow.combinations.link_robots": {
            "verbose_name": "Number of Internal NoFollow Outlinks both in link nofollow and disallowed by robots.txt",
            "group": GROUPS.outlinks_internal_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.nofollow.combinations.meta_robots": {
            "verbose_name": "Number of Internal NoFollow Outlinks both in meta nofollow and disallowed by robots.txt",
            "group": GROUPS.outlinks_internal_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.nb.nofollow.combinations.link_meta_robots": {
            "verbose_name": "Number of Internal NoFollow Outlinks both in link, meta nofollow and disallowed by robots.txt",
            "group": GROUPS.outlinks_internal_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_internal.urls": {
            "verbose_name": "Sample of Internal Outlinks",
            "group": GROUPS.outlinks_internal.name,
            "type": INT_TYPE,
            "settings": {
                ES_NO_INDEX,
                LIST,
                RENDERING.URL_LINK_STATUS,
                FIELD_RIGHTS.SELECT
            },
        },
        "outlinks_internal.urls_exists": {
            "type": BOOLEAN_TYPE,
            "default_value": None
        },

        # external outgoing links (destination is a external url)
        "outlinks_external.nb.total": {
            "verbose_name": "Number of External Outlinks",
            "group": GROUPS.outlinks_external.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_external.nb.unique": {
            "verbose_name": "Unique Number of External Outlinks",
            "group": GROUPS.outlinks_external.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_external.nb.follow.total": {
            "verbose_name": "Number of External Follow Outlinks",
            "group": GROUPS.outlinks_external.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_external.nb.follow.unique": {
            "verbose_name": "Unique Number of External Follow Outlinks",
            "group": GROUPS.outlinks_external.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_external.nb.nofollow.total": {
            "verbose_name": "Number of External NoFollow Outlinks",
            "group": GROUPS.outlinks_external_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_external.nb.nofollow.unique": {
            "verbose_name": "Unique Number of External NoFollow Outlinks",
            "group": GROUPS.outlinks_external_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_external.nb.nofollow.combinations.link": {
            "verbose_name": "Number of External NoFollow Outlinks strictly in link nofollow",
            "group": GROUPS.outlinks_external_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_external.nb.nofollow.combinations.meta": {
            "verbose_name": "Number of External NoFollow Outlinks strictly in meta nofollow",
            "group": GROUPS.outlinks_external_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_external.nb.nofollow.combinations.link_meta": {
            "verbose_name": "Number of External NoFollow Outlinks both in link and meta nofollow",
            "group": GROUPS.outlinks_external_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },

        # outgoing canonical link, one per page
        # if multiple, first one is taken into account
        "canonical.to.url": {
            "verbose_name": "Canonical To",
            "group": GROUPS.canonical.name,
            "type": STRUCT_TYPE,
            "values": {
                "url_str": {"type": "string"},
                "url_id": {"type": "integer"},
            },
            "settings": {
                ES_NO_INDEX,
                RENDERING.URL_STATUS,
                FIELD_RIGHTS.FILTERS_EXIST,
                FIELD_RIGHTS.SELECT,
                URL_ID,
                DIFF_QUALITATIVE
            }
        },
        "canonical.to.equal": {
            "verbose_name": "Canonical is the Same Url",
            "group": GROUPS.canonical.name,
            "type": BOOLEAN_TYPE,
            "settings": {
                AGG_CATEGORICAL,
                DIFF_QUALITATIVE
            }
        },
        "canonical.to.url_exists": {
            "type": "boolean",
            "default_value": None
        },

        # incoming canonical link
        "canonical.from.nb": {
            "verbose_name": "Number of Incoming Canonical",
            "group": GROUPS.canonical.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "canonical.from.urls": {
            "verbose_name": "Canonical From",
            "group": GROUPS.canonical.name,
            "type": INT_TYPE,
            "settings": {
                ES_NO_INDEX,
                LIST,
                RENDERING.URL_STATUS,
                FIELD_RIGHTS.FILTERS_EXIST,
                FIELD_RIGHTS.SELECT,
                URL_ID
            }
        },
        "canonical.from.urls_exists": {
            "type": "boolean",
            "default_value": None
        },

        # outgoing redirection
        "redirect.to.url": {
            "verbose_name": "Redirects to",
            "group": GROUPS.redirects.name,
            "type": STRUCT_TYPE,
            "values": {
                "url_str": {"type": "string"},
                "url_id": {"type": "integer"},
                "http_code": {"type": "integer"}
            },
            "settings": {
                ES_NO_INDEX,
                RENDERING.URL_STATUS,
                FIELD_RIGHTS.FILTERS_EXIST,
                FIELD_RIGHTS.SELECT,
                URL_ID,
                DIFF_QUALITATIVE
            }
        },
        "redirect.to.url_exists": {
            "type": BOOLEAN_TYPE,
            "default_value": None,
        },

        # incoming redirection
        "redirect.from.nb": {
            "verbose_name": "Number of Incoming Redirects",
            "group": GROUPS.redirects.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "redirect.from.urls": {
            "verbose_name": "Redirected From",
            "group": GROUPS.redirects.name,
            "type": INT_TYPE,
            "settings": {
                ES_NO_INDEX,
                LIST,
                RENDERING.URL_HTTP_CODE,
                FIELD_RIGHTS.FILTERS_EXIST,
                FIELD_RIGHTS.SELECT,
                URL_ID
            }
        },
        "redirect.from.urls_exists": {
            "type": "boolean",
            "default_value": None
        }
    }

    def pre_process_document(self, document):
        # store a (dest, is_follow) set of processed links
        document["processed_internal_outlinks"] = set()
        document["processed_external_outlinks"] = set()

    def process_document(self, document, stream):
        url_src, link_type, follow_keys, url_dst, external_url = stream

        if link_type == "a":
            self._process_link(document, stream)
        elif link_type.startswith('r'):
            self._process_redirection(document, stream)
        elif link_type == "canonical":
            self._process_canonical(document, stream)

    def _process_link(self, document, stream):
        url_src, link_type, follow_keys, url_dst, external_url = stream
        # is_internal = url_dst > 0
        is_internal = is_link_internal(follow_keys, url_dst)
        is_follow = len(follow_keys) == 1 and follow_keys[0] == "follow"
        outlink_type = "outlinks_internal" if is_internal else "outlinks_external"
        mask = list_to_mask(follow_keys)

        outlink_nb = document[outlink_type]['nb']
        outlink_nb['total'] += 1

        # target dict changes with link follow status
        follow = outlink_nb['follow' if is_follow else 'nofollow']
        follow['total'] += 1
        if not is_follow:
            # increment nofollow combination counters
            key = _get_nofollow_combination_key(follow_keys)
            follow['combinations'][key] += 1

        # internal outlinks
        # still need dest url id check since we can have internal url
        # blocked by robots.txt
        if is_internal and url_dst > 0:
            #store sample internal links
            outlink_urls = document['outlinks_internal']['urls']
            exists = [url_dst, mask] in outlink_urls
            if len(outlink_urls) < 300 and not exists:
                outlink_urls.append([url_dst, mask])

        #update the set of processed links
        if is_internal:
            if url_dst <= 0:
                #the url is blocked by a robots.txt
                #the url_dst is not meaningful.
                #we replace it by the external_url.
                url_dst = external_url
            # add this link's dest to the processed set
            document['processed_internal_outlinks'].add((url_dst, is_follow))
        else:
            document['processed_external_outlinks'].add((external_url, is_follow))

    def _process_redirection(self, document, stream):
        url_src, link_type, follow_keys, url_dst, external_url = stream
        http_code = link_type[1:]
        redirects_to = document['redirect']['to']
        redirects_to['url'] = {}
        if url_dst == -1:
            redirects_to['url']['url_str'] = external_url
        else:
            redirects_to['url']['url_id'] = url_dst
        redirects_to['url']['http_code'] = int(http_code)
        redirects_to['url_exists'] = True

    def _process_canonical(self, document, stream):
        url_src, link_type, follow_keys, url_dst, external_url = stream
        canonical_to = document['canonical']['to']
        if canonical_to.get('equal', None) is None:
            # We take only the first canonical found
            canonical_to['equal'] = url_src == url_dst
            canonical_to['url'] = {}
            if url_dst > 0:
                canonical_to['url']['url_id'] = url_dst
            else:
                canonical_to['url']['url_str'] = external_url
            canonical_to['url_exists'] = True

    def post_process_document(self, document):
        # If not "outlinks_internal" : we want to store a non-crawled url
        if 'outlinks_internal' in document:
            document['outlinks_internal']['urls_exists'] = len(document['outlinks_internal']['urls']) > 0

            document['outlinks_internal']['nb']['follow']['unique'] = len(
                [url_dst for url_dst, is_follow in document['processed_internal_outlinks'] if is_follow]
            )
            document['outlinks_internal']['nb']['nofollow']['unique'] = len(
                [url_dst for url_dst, is_follow in document['processed_internal_outlinks'] if not is_follow]
            )
            document['outlinks_internal']['nb']['unique'] = len(
                set([url_dst for url_dst, _ in document['processed_internal_outlinks']])
            )
            # delete intermediate data structures
            del document["processed_internal_outlinks"]

        if 'outlinks_external' in document:
            #external_outlinks
            document['outlinks_external']['nb']['follow']['unique'] = len(
                [url_dst for url_dst, is_follow in document['processed_external_outlinks'] if is_follow]
            )
            document['outlinks_external']['nb']['nofollow']['unique'] = len(
                [url_dst for url_dst, is_follow in document['processed_external_outlinks'] if not is_follow]
            )
            document['outlinks_external']['nb']['unique'] = len(
                set([url_dst for url_dst, _ in document['processed_external_outlinks']])
            )
            # delete intermediate data structures
            del document["processed_external_outlinks"]


class InlinksRawStreamDef(StreamDefBase):
    FILE = 'urlinlinks'
    HEADERS = (
        ('id', int),
        ('link_type', str),
        ('bitmask', int),
        ('src_url_id', int),
        ('text_hash', str),
        ('text', str),
    )


class InlinksStreamDef(InlinksRawStreamDef):
    """
    `text` anchors are not always filled in the stream
    They can be found at least one time per `id` and `text_hash`,
    and are not always found in the first iteration of its given `text_hash`
    """
    HEADERS = (
        ('id', int),
        ('link_type', str),
        ('follow', follow_mask),
        ('src_url_id', int),
        ('text_hash', str),
        ('text', str)
    )

    # Check this value if the text is already set
    # If test is an empty string, it will means that it is
    # a real empty link
    TEXT_HASH_ALREADY_SET = '\x00'
    # If the text is really empty, we will flag it as empty
    TEXT_EMPTY = '[empty]'

    URL_DOCUMENT_DEFAULT_GROUP = GROUPS.inlinks.name
    URL_DOCUMENT_MAPPING = {
        # incoming links, must be internal
        "inlinks_internal.nb.total": {
            "verbose_name": "Number of Internal Inlinks",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "inlinks_internal.nb.unique": {
            "verbose_name": "Unique Number of Internal Inlinks",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "inlinks_internal.nb.follow.unique": {
            "verbose_name": "Unique Number of Follow Internal Inlinks",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "inlinks_internal.nb.follow.total": {
            "verbose_name": "Number of Internal Follow Inlinks",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "inlinks_internal.nb.nofollow.unique": {
            "verbose_name": "Unique Number of NoFollow Internal Inlinks",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            }
        },
        "inlinks_internal.nb.nofollow.total": {
            "verbose_name": "Number of Internal NoFollow Inlinks",
            "group": GROUPS.inlinks_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "inlinks_internal.nb.nofollow.combinations.link": {
            "verbose_name": "Number of NoFollow links strictly in link nofollow",
            "group": GROUPS.inlinks_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "inlinks_internal.nb.nofollow.combinations.meta": {
            "verbose_name": "Number of NoFollow links strictly in meta nofollow",
            "group": GROUPS.inlinks_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "inlinks_internal.nb.nofollow.combinations.link_meta": {
            "verbose_name": "Number of NoFollow links both in link and meta nofollow",
            "group": GROUPS.inlinks_nofollow.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "inlinks_internal.urls": {
            "verbose_name": "Sample of internal inlinks urls",
            "group": GROUPS.inlinks.name,
            "type": INT_TYPE,
            "settings": {
                ES_NO_INDEX,
                LIST,
                RENDERING.URL_LINK_STATUS,
                FIELD_RIGHTS.SELECT,
                URL_ID
            }
        },
        "inlinks_internal.urls_exists": {
            "type": "boolean",
            "default_value": None
        },
        "inlinks_internal.anchors.nb": {
            "type": INT_TYPE,
            "verbose_name": "Number of incoming text anchors",
            "group": GROUPS.inlinks.name,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            },
            "enabled": check_enabled("top_anchors")
        },
        "inlinks_internal.anchors.top": {
            "verbose_name": "Top {nb} of incoming text anchors".format(nb=NB_TOP_ANCHORS),
            "type": STRUCT_TYPE,
            "values": STRING_NB_MAP_MAPPING,
            "group": GROUPS.inlinks.name,
            "settings": {
                RENDERING.STRING_NB_MAP,
                FIELD_RIGHTS.SELECT
            },
            "enabled": check_enabled("top_anchors")
        },
        # The following field is already created with the above one (as a STRUCT_TYPE)
        # But we need to return it to request it
        "inlinks_internal.anchors.top.text": {
            "verbose_name": "Incoming text anchors (top {nb})".format(nb=NB_TOP_ANCHORS),
            "type": STRING_TYPE,
            "group": GROUPS.inlinks.name,
            "settings": {
                FAKE_FIELD,
                FIELD_RIGHTS.FILTERS
            },
            "enabled": check_enabled("top_anchors")
        }
    }

    def pre_process_document(self, document):
        # temporary structures for analytic processing
        document["processed_inlink_link"] = set()
        document["tmp_anchors_txt"] = {}
        document["tmp_anchors_nb"] = Counter()

    def process_document(self, document, stream):
        url_dst, link_type, follow_keys, url_src, text_hash, text = stream

        if link_type == "a":
            self._process_link(document, stream)
        elif link_type.startswith('r'):
            self._process_redirection(document, stream)
        elif link_type == "canonical":
            self._process_canonical(document, stream)

    def _process_link(self, document, stream):
        url_dst, link_type, follow_keys, url_src, text_hash, text = stream
        is_follow = len(follow_keys) == 1 and follow_keys[0] == "follow"
        mask = list_to_mask(follow_keys)
        inlink_nb = document['inlinks_internal']['nb']
        inlink_nb['total'] += 1

        follow = inlink_nb['follow' if is_follow else 'nofollow']
        follow['total'] += 1

        # `text` is not always filled, so we have to push it in a temporary
        # dictionnary when found
        if text != self.TEXT_HASH_ALREADY_SET and text_hash not in document['tmp_anchors_txt']:
            if text == '':
                text = self.TEXT_EMPTY
            document['tmp_anchors_txt'][text_hash] = text

        if is_follow:
            # We increment the number of occurrences found for `text_hash` only
            # for follow inlinks
            if text_hash:
                document['tmp_anchors_nb'][text_hash] += 1
        else:
            key = _get_nofollow_combination_key(follow_keys)
            if 'robots' in key:
                logger.warn('Skip `robots` mask in inlink mask')
            else:
                follow['combinations'][key] += 1

        inlink_urls = document['inlinks_internal']['urls']
        exists = [url_src, mask] in inlink_urls
        if len(inlink_urls) < 300 and not exists:
            inlink_urls.append([url_src, mask])

        # add src to processed set
        document['processed_inlink_link'].add((url_src, is_follow))

        document['inlinks_internal']['urls_exists'] = True

    def _process_redirection(self, document, stream):
        url_dst, link_type, follow_keys, url_src, text_hash, text = stream
        # TODO dangerous assumption of crawl's string format to be 'r3xx'
        http_code = int(link_type[1:])
        redirects_from = document['redirect']['from']
        redirects_from['nb'] += 1
        if len(redirects_from['urls']) < 300:
            redirects_from['urls'].append([url_src, http_code])
        redirects_from['urls_exists'] = True

    def _process_canonical(self, document, stream):
        url_dst, link_type, follow_keys, url_src, text_hash, text = stream
        canonical_from = document['canonical']['from']

        # only count for none self canonical
        if url_dst != url_src:
            canonical_from['nb'] += 1
            if len(canonical_from['urls']) < 300:
                canonical_from['urls'].append(url_src)
            canonical_from['urls_exists'] = True

    def post_process_document(self, document):
        # If not "inlinks_internal" : we want to store a non-crawled url
        if not 'inlinks_internal' in document:
            return
        document['inlinks_internal']['nb']['follow']['unique'] = len(
            [url_dst for url_dst, is_follow in document['processed_inlink_link'] if is_follow]
        )
        document['inlinks_internal']['nb']['nofollow']['unique'] = len(
            [url_dst for url_dst, is_follow in document['processed_inlink_link'] if not is_follow]
        )

        document["inlinks_internal"]["anchors"]["top"] = {"text": [], "nb": []}

        # We map the number of occurrences from each `text_hash` to the original text,
        # we don't store the `text_hash` in the final document
        # Elasticsearch would imply to create nested documents, so we push a first list
        # containing texts, and a second one (`nb`) containing number of occurrences
        if document["tmp_anchors_nb"]:
            document["inlinks_internal"]["anchors"]["nb"] = len(document["tmp_anchors_nb"])
            for text_hash, nb in document["tmp_anchors_nb"].most_common(NB_TOP_ANCHORS):
                # An empty hash (0) can be located into canonical or redirection line
                # (we don't check those lines for performances reasons)
                # As it maps to an empty string, we replace it in post processing
                if text_hash == '0' and text_hash not in document["tmp_anchors_txt"]:
                    document["tmp_anchors_txt"][text_hash] = self.TEXT_EMPTY
                document["inlinks_internal"]["anchors"]["top"]["text"].append(document["tmp_anchors_txt"][text_hash])
                document["inlinks_internal"]["anchors"]["top"]["nb"].append(nb)

        document['inlinks_internal']['nb']['unique'] = len(
            set([url_dst for url_dst, _ in document['processed_inlink_link']])
        )
        # delete intermediate data structures
        del document["processed_inlink_link"]
        del document["tmp_anchors_txt"]
        del document["tmp_anchors_nb"]


class OutlinksCountersStreamDef(StreamDefBase):
    FILE = 'url_out_links_counters'
    HEADERS = (
        ('id', int),
        ('follow', follow_mask),
        ('is_internal', _raw_to_bool),
        ('score', int),
        ('score_unique', int),
    )


class OutredirectCountersStreamDef(StreamDefBase):
    FILE = 'url_out_redirect_counters'
    HEADERS = (
        ('id', int),
        ('is_internal', _raw_to_bool)
    )


class OutcanonicalCountersStreamDef(StreamDefBase):
    FILE = 'url_out_canonical_counters'
    HEADERS = (
        ('id', int),
        ('equals', _raw_to_bool)
    )


class InlinksCountersStreamDef(StreamDefBase):
    FILE = 'url_in_links_counters'
    HEADERS = (
        ('id', int),
        ('follow', follow_mask),
        ('count', int),
        ('count_unique', int),
    )


class InredirectCountersStreamDef(StreamDefBase):
    FILE = 'url_in_redirect_counters'
    HEADERS = (
        ('id', int),
        ('score', int)
    )


class IncanonicalCountersStreamDef(StreamDefBase):
    FILE = 'url_in_canonical_counters'
    HEADERS = (
        ('id', int),
        ('score', int)
    )


class BadLinksStreamDef(StreamDefBase):
    FILE = 'urlbadlinks'
    HEADERS = (
        ('id', int),
        ('dst_url_id', int),
        ('http_code', int)
    )
    URL_DOCUMENT_DEFAULT_GROUP = GROUPS.outlinks_internal.name
    URL_DOCUMENT_MAPPING = {
        # erroneous outgoing internal links
        "outlinks_errors.3xx.nb": {
            "type": INT_TYPE,
            "verbose_name": "Number of error links in 3xx",
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_errors.3xx.urls": {
            "type": INT_TYPE,
            "verbose_name": "Sample of error links in 3xx",
            "settings": {
                ES_NO_INDEX,
                LIST,
                FIELD_RIGHTS.SELECT,
                RENDERING.URL,
                URL_ID,
            }
        },
        "outlinks_errors.3xx.urls_exists": {
            "type": "boolean",
            "default_value": None
        },

        "outlinks_errors.4xx.nb": {
            "type": INT_TYPE,
            "verbose_name": "Number of error links in 4xx",
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_errors.4xx.urls": {
            "type": INT_TYPE,
            "verbose_name": "Sample of error links in 4xx",
            "settings": {
                ES_NO_INDEX,
                LIST,
                FIELD_RIGHTS.SELECT,
                RENDERING.URL,
                URL_ID
            }
        },
        "outlinks_errors.4xx.urls_exists": {
            "type": "boolean",
            "default_value": None
        },

        "outlinks_errors.5xx.nb": {
            "type": INT_TYPE,
            "verbose_name": "Number of error links in 5xx",
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_errors.5xx.urls": {
            "type": INT_TYPE,
            "verbose_name": "Sample of error links in 5xx",
            "settings": {
                ES_NO_INDEX,
                LIST,
                FIELD_RIGHTS.SELECT,
                RENDERING.URL,
                URL_ID
            }
        },
        "outlinks_errors.5xx.urls_exists": {
            "type": "boolean",
            "default_value": None
        },
        # total error_links number
        # DEPRECATED: this field has been replaced by
        #"outlinks_errors.total_bad_http_codes"
        "outlinks_errors.total": {
            "type": "integer",
            "verbose_name": "Number of error links in 3xx/4xx/5xx",
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        # total error_links number corresponding to bad http codes.
        "outlinks_errors.total_bad_http_codes": {
            "type": "integer",
            "verbose_name": "Number of error links in 3xx/4xx/5xx",
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            }
        },
    }

    def process_document(self, document, stream_badlinks):
        _, url_dest_id, http_code = stream_badlinks

        errors = document['outlinks_errors']

        error_kind = None
        if 300 <= http_code < 400:
            error_kind = '3xx'
        elif 400 <= http_code < 500:
            error_kind = '4xx'
        elif http_code >= 500:
            error_kind = '5xx'

        errors[error_kind]['nb'] += 1
        error_urls = errors[error_kind]['urls']
        if len(error_urls) < 10:
            error_urls.append(url_dest_id)

        # increment the consolidate value
        errors['total'] += 1
        errors['total_bad_http_codes'] += 1

        errors[error_kind]['urls_exists'] = True


class BadLinksCountersStreamDef(StreamDefBase):
    FILE = 'urlbadlinks_counters'
    HEADERS = (
        ('id', int),
        ('http_code', int),
        ('score', int)
    )


class LinksToNonStrategicStreamDef(StreamDefBase):
    FILE = 'url_non_strategic_links'
    HEADERS = (
        ('id', int),
        ('follow', _raw_to_bool),
        ('dst_url_id', int)
    )
    URL_DOCUMENT_DEFAULT_GROUP = GROUPS.outlinks_internal.name
    URL_DOCUMENT_MAPPING = {
        "outlinks_errors.non_strategic.urls": {
            "type": INT_TYPE,
            "verbose_name": "Sample of error links to not strategic urls.",
            "settings": {
                ES_NO_INDEX,
                LIST,
                FIELD_RIGHTS.SELECT,
                RENDERING.URL,
                URL_ID
            }
        },
        "outlinks_errors.non_strategic.urls_exists": {
            "type": "boolean",
            "default_value": None
        },
    }

    def process_document(self, document, stream_non_strategic_links):
        _, follow, url_dest_id = stream_non_strategic_links

        errors = document['outlinks_errors']
        error_kind = "non_strategic"
        error_urls = errors[error_kind]['urls']

        # capture 10 follow samples
        if len(error_urls) < 10 and follow:
            error_urls.append(url_dest_id)

        errors[error_kind]['urls_exists'] = True


class LinksToNonStrategicCountersStreamDef(StreamDefBase):
    FILE = 'url_non_strategic_links_counters'
    HEADERS = (
        ('id', int),
        ('follow_unique', int),
        ('follow_total', int),
    )
    URL_DOCUMENT_DEFAULT_GROUP = GROUPS.outlinks_internal.name
    URL_DOCUMENT_MAPPING = {
        # erroneous outgoing internal links
        "outlinks_errors.non_strategic.nb.follow.unique": {
            "type": INT_TYPE,
            "verbose_name": "Number of unique error links to not strategic urls.",
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            }
        },
        "outlinks_errors.non_strategic.nb.follow.total": {
            "type": INT_TYPE,
            "verbose_name": "Number of error links to not strategic urls.",
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            }
        }
    }

    def process_document(self, document, stream_non_strategic_counters):
        _, unique, total = stream_non_strategic_counters

        errors = document['outlinks_errors']
        error_kind = "non_strategic"
        non_strategic = errors[error_kind]
        non_strategic['nb']['follow']['unique'] = unique
        non_strategic['nb']['follow']['total'] = total


class InlinksPercentilesStreamDef(StreamDefBase):
    FILE = 'inlinks_percentiles'
    HEADERS = (
        ('id', int),
        ('percentile_id', int),
        ('nb_follow_inlinks', int)
    )

    URL_DOCUMENT_DEFAULT_GROUP = GROUPS.inlinks.name
    URL_DOCUMENT_MAPPING = {
        # erroneous outgoing internal links
        "inlinks_internal.percentile": {
            "type": INT_TYPE,
            "verbose_name": "Inlinks Percentile",
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                AGG_CATEGORICAL,
                FIELD_RIGHTS.FILTERS,
                FIELD_RIGHTS.SELECT,
                DIFF_QUANTITATIVE
            }
        },
    }

    def process_document(self, document, input_stream):
        _, percentile_id, _ = input_stream
        document["inlinks_internal"]["percentile"] = percentile_id
