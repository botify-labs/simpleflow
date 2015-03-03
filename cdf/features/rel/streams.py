from cdf.compat import json

from cdf.features.main.reasons import (
    decode_reason_mask,
    Reasons
)
from cdf.metadata.url.url_metadata import (
    LONG_TYPE, INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    DATE_TYPE, ES_NO_INDEX, ES_NOT_ANALYZED, ES_DOC_VALUE,
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

from cdf.features.main.compliant_url import make_compliant_bitarray


__all__ = ["RealStreamDef"]


def bool_int(value):
    if value == '':
        return None
    elif value == '1':
        return True
    return False


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

class RelCompliantStreamDef(StreamDefBase):
    FILE = 'urlrelcompliant'
    HEADERS = (
        ('id', int),
        ('type', int),
        ('mask', int),
        ('url_id_dest', int),
        ('url_dest', str),
        ('value', str),
        ('url_dest_compliant', bool_int)
    )
    URL_DOCUMENT_DEFAULT_GROUP = "hreflang"
    URL_DOCUMENT_MAPPING = {
        "rel.hreflang.out.nb": {
            "verbose_name": "Outgoing Number of Href Langs",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE
            }
        },
        "rel.hreflang.out.valid.nb": {
            "verbose_name": "Outgoing Number of Valid Href Langs",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE
            }
        },
        "rel.hreflang.out.valid.langs": {
            "verbose_name": "Outgoing Valid Href Langs",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.out.valid.has_warning": {
            "verbose_name": "Outgoing Href Langs Has Warning",
            "type": BOOLEAN_TYPE,
        },
        "rel.hreflang.out.valid.warning": {
            "verbose_name": "Outgoing Href Langs Warning codes",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.out.valid.values": {
            "verbose_name": "Outgoing Valid Href Langs URLs",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT
            ]
        },
        "rel.hreflang.out.not_valid.nb": {
            "verbose_name": "Outgoing Number of Not Valid Href Langs",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE
            }
        },
        "rel.hreflang.out.not_valid.errors": {
            "verbose_name": "Outgoing Not Valid Href Langs Errors",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.out.not_valid.values": {
            "verbose_name": "Outgoing Not Valid Href Langs URLs",
            "type": STRING_TYPE,
            "settings": [
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT
            ]
        }
    }

    def pre_process_document(self, document):
        # store a (dest, is_follow) set of processed links
        document["hreflang_errors"] = set()
        document["hreflang_warning"] = set()
        document["hreflang_errors_samples"] = []
        document["hreflang_valid_samples"] = []

    def process_document(self, document, stream):
        if rel_constants.REL_TYPES[stream[1]] == rel_constants.REL_HREFLANG:
            self.process_hreflang(document, stream)

    def process_hreflang(self, document, stream):
        subdoc = document["rel"]["hreflang"]["out"]
        subdoc["nb"] += 1

        mask = stream[2]
        iso_codes = stream[5]
        url_id_dest = stream[3]
        dest_compliant = stream[6]
        country = get_country(iso_codes)
        errors = set()
        warning = set()
        if not is_lang_valid(iso_codes):
            errors.add(rel_constants.ERROR_LANG_NOT_RECOGNIZED)
        if country and not is_country_valid(country):
            errors.add(rel_constants.ERROR_COUNTRY_NOT_RECOGNIZED)
        if url_id_dest > -1 and not dest_compliant:
            errors.add(rel_constants.ERROR_DEST_NOT_COMPLIANT)

        if mask & rel_constants.MASK_NOFOLLOW_ROBOTS_TXT == rel_constants.MASK_NOFOLLOW_ROBOTS_TXT:
            warning.add(rel_constants.WARNING_DEST_BLOCKED_ROBOTS_TXT)
        if mask & rel_constants.MASK_NOFOLLOW_CONFIG == rel_constants.MASK_NOFOLLOW_CONFIG:
            warning.add(rel_constants.WARNING_DEST_BLOCKED_CONFIG)

        # url_id not found and mask has not robot or config flags
        if (url_id_dest == -1 and
            mask & (rel_constants.MASK_NOFOLLOW_ROBOTS_TXT + rel_constants.MASK_NOFOLLOW_CONFIG) == 0):
            warning.add(rel_constants.WARNING_DEST_NOT_CRAWLED)

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
            if len(document["hreflang_errors_samples"]) < rel_constants.MAX_HREFLANG_OUT_ERRORS:
                document["hreflang_errors_samples"].append(sample)
        else:
            sample = {
                "lang": iso_codes,
                "warning": list(warning),
            }
            if url_id_dest != -1:
                sample["url_id"] = url_id_dest
            else:
                sample["url"] = stream[4]

            if len(document["hreflang_valid_samples"]) < rel_constants.MAX_HREFLANG_OUT_VALID:
                document["hreflang_valid_samples"].append(sample)
            if warning:
                subdoc["valid"]["has_warning"] = True
                document["hreflang_warning"] |= warning
            subdoc["valid"]["nb"] += 1
            if iso_codes not in subdoc["valid"]["langs"]:
                subdoc["valid"]["langs"].append(iso_codes)

    def post_process_document(self, document):
        # Store the final errors lists
        document["rel"]["hreflang"]["out"]["not_valid"]["errors"] = list(document["hreflang_errors"])
        document["rel"]["hreflang"]["out"]["valid"]["warning"] = list(document["hreflang_warning"])
        document["rel"]["hreflang"]["out"]["not_valid"]["values"] = json.dumps(document["hreflang_errors_samples"])
        document["rel"]["hreflang"]["out"]["valid"]["values"] = json.dumps(document["hreflang_valid_samples"])
        del document["hreflang_errors"]
        del document["hreflang_warning"]
        del document["hreflang_errors_samples"]
        del document["hreflang_valid_samples"]


class InRelStreamDef(StreamDefBase):
    FILE = 'urlinrel'
    HEADERS = (
        ('id', int),
        ('type', int),
        ('mask', int),
        ('url_id_src', int),
        ('value', str),
    )
    URL_DOCUMENT_DEFAULT_GROUP = "hreflang"
    URL_DOCUMENT_MAPPING = {
        "rel.hreflang.in.nb": {
            "verbose_name": "Incoming Number of Href Langs",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE
            }
        },
        "rel.hreflang.in.valid.nb": {
            "verbose_name": "Incoming Number of Valid Href Langs",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE
            }
        },
        "rel.hreflang.in.valid.langs": {
            "verbose_name": "Incoming Valid Href Langs",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.in.valid.has_warning": {
            "verbose_name": "Incoming Href Langs Has Warning",
            "type": BOOLEAN_TYPE,
        },
        "rel.hreflang.in.valid.warning": {
            "verbose_name": "Incoming Href Langs Warning codes",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.in.valid.values": {
            "verbose_name": "Incoming Valid Href Langs URLs",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT
            ]
        },
        "rel.hreflang.in.not_valid.nb": {
            "verbose_name": "Incoming Number of Not Valid Href Langs",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE
            }
        },
        "rel.hreflang.in.not_valid.errors": {
            "verbose_name": "Incoming Not Valid Href Langs Errors",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.in.not_valid.values": {
            "verbose_name": "Incoming Not Valid Href Langs URLs",
            "type": STRING_TYPE,
            "settings": [
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT
            ]
        }
    }

    def pre_process_document(self, document):
        # store a (dest, is_follow) set of processed links
        document["inhreflang_errors"] = set()
        document["inhreflang_errors_samples"] = []

    def process_document(self, document, stream):
        if rel_constants.REL_TYPES[stream[1]] == rel_constants.REL_HREFLANG:
            self.process_hreflang(document, stream)

    def process_hreflang(self, document, stream):
        subdoc = document["rel"]["hreflang"]["in"]
        subdoc["nb"] += 1

        mask = stream[2]
        iso_codes = stream[5]
        lang = iso_codes[0:2]
        url_id_dest = stream[3]
        dest_compliant = stream[6]
        country = get_country(iso_codes)
        errors = set()
        warning = set()
        if not is_lang_valid(iso_codes):
            errors.add(rel_constants.ERROR_LANG_NOT_RECOGNIZED)
        if country and not is_country_valid(country):
            errors.add(rel_constants.ERROR_COUNTRY_NOT_RECOGNIZED)
        if document["lang"] != "notset" and document["lang"] != lang:
            errors.add(rel_constants.ERROR_LANG_NOT_EQUAL)

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
            if len(document["in_hreflang_errors_samples"]) < rel_constants.MAX_HREFLANG_OUT_ERRORS:
                document["in_hreflang_errors_samples"].append(sample)
        else:
            sample = {
                "lang": iso_codes,
            }
            if url_id_dest != -1:
                sample["url_id"] = url_id_dest
            else:
                sample["url"] = stream[4]

            if len(document["in_hreflang_valid_samples"]) < rel_constants.MAX_HREFLANG_IN_VALID:
                document["in_hreflang_valid_samples"].append(sample)
            subdoc["valid"]["nb"] += 1
            if iso_codes not in subdoc["valid"]["langs"]:
                subdoc["valid"]["langs"].append(iso_codes)


