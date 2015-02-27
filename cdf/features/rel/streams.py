from cdf.compat import json

from cdf.features.main.reasons import (
    decode_reason_mask,
    Reasons
)
from cdf.metadata.url.url_metadata import (
    LONG_TYPE, INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    DATE_TYPE, ES_NOT_ANALYZED, ES_DOC_VALUE,
    LIST, AGG_CATEGORICAL, AGG_NUMERICAL, URL_ID,
    DIFF_QUALITATIVE, DIFF_QUANTITATIVE
)
from cdf.core.streams.exceptions import GroupWithSkipException
from cdf.core.streams.base import StreamDefBase
from cdf.utils.date import date_2k_mn_to_date
from cdf.utils.hashing import string_to_int64
from cdf.core.metadata.constants import RENDERING, FIELD_RIGHTS
from cdf.core.metadata.dataformat import check_enabled

from cdf.features.rel import constants as rel_constants

from cdf.features.rel.utils import (
    is_lang_valid,
    get_country,
    is_country_valid
)

__all__ = ["RealStreamDef"]


class RelStreamDef(StreamDefBase):
    FILE = 'urlrel'
    HEADERS = (
        ('id', int),
        ('type', int),
        ('mask', int),
        ('url_id_dest', int),
        ('url_dest', str),
        ('value', str)
    )
    URL_DOCUMENT_DEFAULT_GROUP = "hreflang"
    URL_DOCUMENT_MAPPING = {
        "rel.hreflang.out.nb": {
            "verbose_name": "Outgoing Number of Href Langs",
            "type": INT_TYPE,
        },
        "rel.hreflang.out.valid.nb": {
            "verbose_name": "Outgoing Number of Valid Href Langs",
            "type": INT_TYPE,
        },
        "rel.hreflang.out.valid.langs": {
            "verbose_name": "Outgoing Valid Href Langs",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED
            ]
        },
        "rel.hreflang.out.valid.warning": {
            "verbose_name": "Outgoing Href Langs Warning codes",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED
            ]
        },
        "rel.hreflang.out.valid.samples": {
            "verbose_name": "Outgoing Valid Href Langs URLs",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                FIELD_RIGHTS.SELECT
            ]
        },
        "rel.hreflang.out.not_valid.nb": {
            "verbose_name": "Outgoing Number of Not Valid Href Langs",
            "type": INT_TYPE,
        },
        "rel.hreflang.out.not_valid.errors": {
            "verbose_name": "Outgoing Not Valid Href Langs Errors",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
            ]
        },
        "rel.hreflang.out.not_valid.samples": {
            "verbose_name": "Outgoing Not Valid Href Langs URLs",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                FIELD_RIGHTS.SELECT
            ]
        }
    }


    def pre_process_document(self, document):
        # store a (dest, is_follow) set of processed links
        document["hreflang_errors"] = set()
        document["hreflang_warning"] = set()

    def process_document(self, document, stream):
        if rel_constants.REL_TYPES[stream[1]] == rel_constants.REL_HREFLANG:
            self.process_hreflang(document, stream)

    def process_hreflang(self, document, stream):
        subdoc = document["rel"]["hreflang"]["out"]
        subdoc["nb"] += 1

        mask = stream[2]
        iso_codes = stream[5]
        url_id_dest = stream[3]
        country = get_country(iso_codes)
        errors = set()
        warning = set()
        if not is_lang_valid(iso_codes):
            errors.add(rel_constants.LANG_NOT_RECOGNIZED)
        if country and not is_country_valid(country):
            errors.add(rel_constants.COUNTRY_NOT_RECOGNIZED)
        if mask & rel_constants.MASK_NOFOLLOW_ROBOTS_TXT == rel_constants.MASK_NOFOLLOW_ROBOTS_TXT:
            warning.add(rel_constants.DEST_BLOCKED_ROBOTS_TXT)
        if mask & rel_constants.MASK_NOFOLLOW_CONFIG == rel_constants.MASK_NOFOLLOW_CONFIG:
            warning.add(rel_constants.DEST_BLOCKED_CONFIG)
        # url_id not found and mask has not robot or config flags
        if (url_id_dest == -1 and
            mask & (rel_constants.MASK_NOFOLLOW_ROBOTS_TXT + rel_constants.MASK_NOFOLLOW_CONFIG) == 0):
            warning.add(rel_constants.DEST_NOT_CRAWLED)

        if errors:
            subdoc["not_valid"]["nb"] += 1
            document["hreflang_errors"] |= errors
            sample = {
                "errors": list(errors),
                "value": iso_codes,
            }
            if url_id_dest != -1:
                sample["url_id"] = url_id_dest
            else:
                sample["url"] = stream[4]
            subdoc["not_valid"]["samples"].append(json.dumps(sample))
        else:
            sample = {
                "lang": iso_codes,
                "warning": list(warning),
            }
            if url_id_dest != -1:
                sample["url_id"] = url_id_dest
            else:
                sample["url"] = stream[4]

            subdoc["valid"]["samples"].append(json.dumps(sample))
            if warning:
                document["hreflang_warning"] |= warning
            subdoc["valid"]["nb"] += 1
            if iso_codes not in subdoc["valid"]["langs"]:
                subdoc["valid"]["langs"].append(iso_codes)


    def post_process_document(self, document):
        # Store the final errors lists
        document["rel"]["hreflang"]["out"]["not_valid"]["errors"] = list(document["hreflang_errors"])
        document["rel"]["hreflang"]["out"]["valid"]["warning"] = list(document["hreflang_warning"])
        del document["hreflang_errors"]
        del document["hreflang_warning"]
