from cdf.compat import json

from cdf.metadata.url.url_metadata import (
    INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    ES_NO_INDEX, ES_NOT_ANALYZED, ES_DOC_VALUE,
    LIST, AGG_CATEGORICAL, AGG_NUMERICAL, URL_ID,
    DIFF_QUALITATIVE, DIFF_QUANTITATIVE
)
from cdf.core.streams.base import StreamDefBase
from cdf.core.metadata.constants import RENDERING, FIELD_RIGHTS
from cdf.features.rel import constants as rel_constants

from cdf.features.rel.utils import (
    is_lang_valid,
    extract_lang_and_region,
    is_region_valid
)


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
            "verbose_name": "Number of Outgoing Hreflang",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "rel.hreflang.out.valid.nb": {
            "verbose_name": "Number of Outgoing Valid Hreflang",
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
                ES_DOC_VALUE,
                RENDERING.NOT_SORTABLE
            ]
        },
        "rel.hreflang.out.valid.regions": {
            "verbose_name": "Outgoing Valid Hreflang Langs+Regions",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE,
                RENDERING.NOT_SORTABLE
            ]
        },
        "rel.hreflang.out.valid.sends_x-default": {
            "verbose_name": "Outgoing Hreflang Sends x-default",
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
            "verbose_name": "Outgoing Hreflang Warning Reasons",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE,
                RENDERING.NOT_SORTABLE
            ]
        },
        "rel.hreflang.out.valid.values": {
            "verbose_name": "Outgoing Valid Hreflang Values",
            "type": STRING_TYPE,  # JSON encoded. Contains list of dict.
            "settings": [
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT,
                RENDERING.HREFLANG_VALID_VALUES,
                RENDERING.NOT_SORTABLE
            ]
        },
        "rel.hreflang.out.not_valid.nb": {
            "verbose_name": "Number of Outgoing Not Valid Hreflang",
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
                ES_DOC_VALUE,
                RENDERING.NOT_SORTABLE
            ]
        },
        "rel.hreflang.out.not_valid.values": {
            "verbose_name": "Outgoing Not Valid Hreflang Values",
            "type": STRING_TYPE,
            "settings": [
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT,
                RENDERING.HREFLANG_ERROR_VALUES,
                RENDERING.NOT_SORTABLE
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
        iso_codes = stream[5].lower()
        lang, region = extract_lang_and_region(iso_codes)
        url_id_dest = stream[3]
        dest_compliant = stream[6]
        errors = set()
        warning = set()

        if iso_codes != "x-default" and not is_lang_valid(lang):
            errors.add(rel_constants.ERROR_LANG_NOT_RECOGNIZED)
        if iso_codes != "x-default" and region and not is_region_valid(region):
            errors.add(rel_constants.ERROR_REGION_NOT_RECOGNIZED)
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
                    subdoc["valid"]["langs"].append(lang)

    def post_process_document(self, document):
        if "hreflang_warning" in document:
            document["rel"]["hreflang"]["out"]["valid"]["warning"] = list(
                document["hreflang_warning"]
            )
            del document["hreflang_warning"]

        if "hreflang_valid_samples" in document:
            document["rel"]["hreflang"]["out"]["valid"]["values"] = json.dumps(
                document["hreflang_valid_samples"]
            )
            del document["hreflang_valid_samples"]

        if "hreflang_errors" in document:
            document["rel"]["hreflang"]["out"]["not_valid"]["errors"] = list(
                document["hreflang_errors"]
            )
            del document["hreflang_errors"]

            document["rel"]["hreflang"]["out"]["not_valid"]["values"] = json.dumps(
                document["hreflang_errors_samples"]
            )
            del document["hreflang_errors_samples"]


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
            "verbose_name": "Number of Incoming Hreflang",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "rel.hreflang.in.valid.nb": {
            "verbose_name": "Number of Incoming Valid Hreflang",
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            }
        },
        "rel.hreflang.in.valid.receives_x-default": {
            "verbose_name": "URL Receives x-default From Incoming Hreflang",
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
                ES_DOC_VALUE,
                RENDERING.NOT_SORTABLE
            ]
        },
        "rel.hreflang.in.valid.regions": {
            "verbose_name": "Incoming Valid Hreflang Lang+Region",
            "help_text": "Format ISO 3166-1 Alpha 2 for regions",
            "type": STRING_TYPE,
            "settings": [
                LIST,
                ES_NOT_ANALYZED,
                ES_DOC_VALUE,
                RENDERING.NOT_SORTABLE
            ]
        },
        "rel.hreflang.in.valid.values": {
            "verbose_name": "Incoming Valid Hreflang Values",
            "type": STRING_TYPE,  # JSON encoded. Contains list of dict.
            "settings": [
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT,
                RENDERING.HREFLANG_VALID_VALUES,
                RENDERING.NOT_SORTABLE
            ]
        },
        "rel.hreflang.in.not_valid.nb": {
            "verbose_name": "Number of Incoming Not Valid Hreflang",
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
                ES_DOC_VALUE,
                RENDERING.NOT_SORTABLE
            ]
        },
        "rel.hreflang.in.not_valid.values": {
            "verbose_name": "Incoming Not Valid Hreflang Values",
            "type": STRING_TYPE,
            "settings": [
                ES_NO_INDEX,
                FIELD_RIGHTS.SELECT,
                RENDERING.HREFLANG_ERROR_VALUES,
                RENDERING.NOT_SORTABLE
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
        iso_codes = stream[4].lower()
        lang, region = extract_lang_and_region(iso_codes)
        url_id_src = stream[3]
        errors = set()

        document_lang, document_region = extract_lang_and_region(
                document["lang"].lower()
        )

        if document_lang in ("notset", "?"):
            errors.add(rel_constants.ERROR_LANG_NOT_SET)
        elif iso_codes != "x-default" and not is_lang_valid(lang):
            errors.add(rel_constants.ERROR_LANG_NOT_RECOGNIZED)
        elif iso_codes != "x-default" and document_lang != lang:
            errors.add(rel_constants.ERROR_LANG_NOT_EQUAL)
        elif (
                iso_codes != "x-default" and
                document_region and
                region and
                document_region != region
        ):
            errors.add(rel_constants.ERROR_REGION_NOT_EQUAL)

        if not document["strategic"]["is_strategic"]:
            errors.add(rel_constants.ERROR_NOT_COMPLIANT)

        if region and not is_region_valid(region):
            errors.add(rel_constants.ERROR_REGION_NOT_RECOGNIZED)

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
