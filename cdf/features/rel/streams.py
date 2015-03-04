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


__all__ = ["RelStreamDef", "RelCompliantStreamDef", "InRelStreamDef"]


def bool_int(value):
    if value == '':
        return None
    elif value == '1':
        return True
    return False


class RelStreamDef(StreamDefBase):
    FILE = 'urllinkrel'
    HEADERS = (
        ('id', int),
        ('type', int),
        ('mask', int),
        ('url_id_dest', int),
        ('url_dest', str),
        ('value', str)
    )

class RelCompliantStreamDef(StreamDefBase):
    FILE = 'urllinkrelcompliant'
    HEADERS = (
        ('id', int),
        ('type', int),
        ('mask', int),
        ('url_id_dest', int),
        ('url_dest', str),
        ('value', str),
        ('url_dest_compliant', bool_int)
    )
    URL_DOCUMENT_DEFAULT_GROUP = "hreflang_outgoing"
    URL_DOCUMENT_MAPPING = {
        "rel.hreflang.out.nb": {
            "verbose_name": "Outgoing Number of Href Langs",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "rel.hreflang.out.valid.nb": {
            "verbose_name": "Outgoing Number of Valid Hreflang",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "rel.hreflang.out.valid.langs": {
            "verbose_name": "Outgoing Valid Hreflang Langs",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.out.valid.regions": {
            "verbose_name": "Outgoing Valid Hreflang Langs+Regions",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.out.valid.sends_x-default": {
            "verbose_name": "Outgoing Hreflang Has Warning",
            "type": BOOLEAN_TYPE,
            "settings": {
                DIFF_QUALITATIVE,
                AGG_CATEGORICAL
            }
        },
        "rel.hreflang.out.valid.has_warning": {
            "verbose_name": "Outgoing Hreflang Has Warning",
            "type": BOOLEAN_TYPE,
            "settings": {
                DIFF_QUALITATIVE,
                AGG_CATEGORICAL
            }
        },
        "rel.hreflang.out.valid.warning": {
            "verbose_name": "Outgoing Hreflang Warning codes",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.out.valid.values": {
            "verbose_name": "Outgoing Valid Hreflang Values",
            "type": STRING_TYPE,
            "settings": [
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT,
                RENDERING.HREFLANG_VALID_VALUES
            ]
        },
        "rel.hreflang.out.not_valid.nb": {
            "verbose_name": "Outgoing Number of Not Valid Hreflang",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "rel.hreflang.out.not_valid.errors": {
            "verbose_name": "Outgoing Not Valid Hreflang Errors",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.out.not_valid.values": {
            "verbose_name": "Outgoing Not Valid Hreflang Values",
            "type": STRING_TYPE,
            "settings": [
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT,
                RENDERING.HREFLANG_ERROR_VALUES
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
        if iso_codes != "x-default" and not is_lang_valid(iso_codes):
            errors.add(rel_constants.ERROR_LANG_NOT_RECOGNIZED)
        if iso_codes != "x-default" and country and not is_country_valid(country):
            errors.add(rel_constants.ERROR_COUNTRY_NOT_RECOGNIZED)
        if url_id_dest > -1 and not dest_compliant:
            errors.add(rel_constants.ERROR_DEST_NOT_COMPLIANT)

        if mask & rel_constants.MASK_NOFOLLOW_ROBOTS_TXT == rel_constants.MASK_NOFOLLOW_ROBOTS_TXT:
            warning.add(rel_constants.WARNING_DEST_BLOCKED_ROBOTS_TXT)
        if mask & rel_constants.MASK_NOFOLLOW_CONFIG == rel_constants.MASK_NOFOLLOW_CONFIG:
            warning.add(rel_constants.WARNING_DEST_BLOCKED_CONFIG)

        # url_id not found and mask has not robot or config flags
        if (url_id_dest == -1 and
            mask & (rel_constants.MASK_NOFOLLOW_ROBOTS_TXT | rel_constants.MASK_NOFOLLOW_CONFIG) == 0):
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
                "value": iso_codes,
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

            if iso_codes == "x-default":
                subdoc["valid"]["sends_x-default"] = True
            elif iso_codes not in subdoc["valid"]["regions"]:
                subdoc["valid"]["regions"].append(iso_codes)
                if iso_codes[0:2] not in subdoc["valid"]["langs"]:
                    subdoc["valid"]["langs"].append(iso_codes[0:2])

    def post_process_document(self, document):
        # Store the final errors lists
        if not "hreflang_errors" in document:
            # No stream available for this entry, nothing to delete
            return

        document["rel"]["hreflang"]["out"]["not_valid"]["errors"] = list(document["hreflang_errors"])
        document["rel"]["hreflang"]["out"]["valid"]["warning"] = list(document["hreflang_warning"])
        document["rel"]["hreflang"]["out"]["not_valid"]["values"] = json.dumps(document["hreflang_errors_samples"])
        document["rel"]["hreflang"]["out"]["valid"]["values"] = json.dumps(document["hreflang_valid_samples"])
        del document["hreflang_errors"]
        del document["hreflang_warning"]
        del document["hreflang_errors_samples"]
        del document["hreflang_valid_samples"]


class InRelStreamDef(StreamDefBase):
    FILE = 'urlinlinkrel'
    HEADERS = (
        ('id', int),
        ('type', int),
        ('mask', int),
        ('url_id_src', int),
        ('value', str),
    )
    URL_DOCUMENT_DEFAULT_GROUP = "hreflang_incoming"
    URL_DOCUMENT_MAPPING = {
        "rel.hreflang.in.nb": {
            "verbose_name": "Incoming Number of Href Langs",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "rel.hreflang.in.valid.nb": {
            "verbose_name": "Incoming Number of Valid Href Langs",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "rel.hreflang.in.valid.receives_x-default": {
            "verbose_name": "URL receives x-default from incoming Href Langs",
            "type": BOOLEAN_TYPE,
            "settings": {
                DIFF_QUALITATIVE,
                AGG_CATEGORICAL
            }
        },
        "rel.hreflang.in.valid.langs": {
            "verbose_name": "Incoming Valid Hreflang Langs",
            "help_text": "Format ISO 639-1 for langs",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.in.valid.regions": {
            "verbose_name": "Incoming Valid Hreflang Lang+Region",
            "help_text": "Format ISO 3166-1 Alpha 2 for regions",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.in.valid.values": {
            "verbose_name": "Incoming Valid Hreflang Values",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT,
                RENDERING.HREFLANG_VALID_VALUES
            ]
        },
        "rel.hreflang.in.not_valid.nb": {
            "verbose_name": "Incoming Number of Not Valid Hreflang",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "rel.hreflang.in.not_valid.errors": {
            "verbose_name": "Incoming Not Valid Hreflang Errors",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE
            ]
        },
        "rel.hreflang.in.not_valid.values": {
            "verbose_name": "Incoming Not Valid Hreflang Values",
            "type": STRING_TYPE,
            "settings": [
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT,
                RENDERING.HREFLANG_ERROR_VALUES
            ]
        }
    }

    def pre_process_document(self, document):
        document["inhreflang_errors"] = set()
        document["inhreflang_errors_samples"] = []
        document["inhreflang_valid_samples"] = []
        document["inhreflang_entries"] = []

    def process_document(self, document, stream):
        if rel_constants.REL_TYPES[stream[1]] == rel_constants.REL_HREFLANG:
            self.process_hreflang(document, stream)

    def process_hreflang(self, document, stream):
        # As we don't have access to the current lang now
        # (we loose feature priority on `group_with`)
        # we need to store all streams temporarly
        # and process hreflang in post_process step
        document["inhreflang_entries"].append(stream)

    def post_process_hreflang(self, document, stream):
        subdoc = document["rel"]["hreflang"]["in"]
        subdoc["nb"] += 1

        mask = stream[2]
        iso_codes = stream[4]
        lang = iso_codes[0:2]
        url_id_src = stream[3]
        country = get_country(iso_codes)
        errors = set()

        if document["lang"] in ("notset", "?"):
            errors.add(rel_constants.ERROR_LANG_NOT_SET)
        elif iso_codes != "x-default" and not is_lang_valid(iso_codes):
            errors.add(rel_constants.ERROR_LANG_NOT_RECOGNIZED)
        elif iso_codes != "x-default" and document["lang"] != lang:
            errors.add(rel_constants.ERROR_LANG_NOT_EQUAL)

        if not document["strategic"]["is_strategic"]:
            errors.add(rel_constants.ERROR_NOT_COMPLIANT)

        if country and not is_country_valid(country):
            errors.add(rel_constants.ERROR_COUNTRY_NOT_RECOGNIZED)

        if errors:
            subdoc["not_valid"]["nb"] += 1
            document["inhreflang_errors"] |= errors
            sample = {
                "errors": list(errors),
                "value": iso_codes,
            }
            sample["url_id"] = url_id_src
            if len(document["inhreflang_errors_samples"]) < rel_constants.MAX_HREFLANG_IN_ERRORS:
                document["inhreflang_errors_samples"].append(sample)
        else:
            sample = {
                "value": iso_codes,
            }
            sample["url_id"] = url_id_src

            if len(document["inhreflang_valid_samples"]) < rel_constants.MAX_HREFLANG_IN_VALID:
                document["inhreflang_valid_samples"].append(sample)
            subdoc["valid"]["nb"] += 1

            if iso_codes == "x-default":
                subdoc["valid"]["receives_x-default"] = True
            elif iso_codes not in subdoc["valid"]["regions"]:
                subdoc["valid"]["regions"].append(iso_codes)
                if iso_codes[0:2] not in subdoc["valid"]["langs"]:
                    subdoc["valid"]["langs"].append(iso_codes[0:2])

    def post_process_document(self, document):
        # Store the final errors lists
        if not "inhreflang_entries" in document:
            # No streams for this entry, nothing to delete
            return

        for stream in document["inhreflang_entries"]:
            self.post_process_hreflang(document, stream)

        document["rel"]["hreflang"]["in"]["not_valid"]["errors"] = list(document["inhreflang_errors"])
        document["rel"]["hreflang"]["in"]["not_valid"]["values"] = json.dumps(document["inhreflang_errors_samples"])
        document["rel"]["hreflang"]["in"]["valid"]["values"] = json.dumps(document["inhreflang_valid_samples"])
        del document["inhreflang_errors"]
        del document["inhreflang_errors_samples"]
        del document["inhreflang_valid_samples"]
        del document["inhreflang_entries"]
