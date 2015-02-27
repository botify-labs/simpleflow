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
        "rel.hreflang.out.valid.urls": {
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
        "rel.hreflang.out.not_valid.urls": {
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

    def process_document(self, document, stream):
        if rel_constants.REL_TYPES[stream[1]] == rel_constants.REL_HREFLANG:
            self.process_hreflang(document, stream)

    def process_hreflang(self, document, stream):
        subdoc = document["rel"]["hreflang"]["out"]
        subdoc["nb"] += 1

        iso_codes = stream[5]
        url_id_dest = stream[3]
        country = get_country(iso_codes)
        errors = set()
        if not is_lang_valid(iso_codes):
            errors.add(rel_constants.LANG_NOT_RECOGNIZED)
        if country and not is_country_valid(country):
            errors.add(rel_constants.COUNTRY_NOT_RECOGNIZED)
        #if url_id_dest != -1 and not is_compliant_url(url_id_dest, self.compliants_urls_bitarray):
        #    errors.append(rel_constants.DEST_NOT_COMPLIANT)
        # TODO : DEST_DISALLED_ROBOTS_TXT

        if errors:
            subdoc["not_valid"]["nb"] += 1
            document["hreflang_errors"] |= errors
        else:
            subdoc["valid"]["nb"] += 1
            if iso_codes not in subdoc["valid"]["langs"]:
                subdoc["valid"]["langs"].append(iso_codes)


    def post_process_document(self, document):
        # Store the final errors lists
        document["rel"]["hreflang"]["out"]["not_valid"]["errors"] = list(document["hreflang_errors"])
        del document["hreflang_errors"]
