from cdf.metadata.url.url_metadata import (
    LONG_TYPE, INT_TYPE, STRING_TYPE, BOOLEAN_TYPE,
    DATE_TYPE, FLOAT_TYPE,
    ES_NOT_ANALYZED, ES_DOC_VALUE,
    LIST, AGG_CATEGORICAL, AGG_NUMERICAL, URL_ID,
    DIFF_QUALITATIVE, DIFF_QUANTITATIVE
)

from cdf.core.streams.base import StreamDefBase

__all__ = ["ExtractResultsStreamDef"]

_EXTRACT_RESULT_COUNT = 5


def _generate_ers_document_mapping():
    """
    ExtractResultsStreamDef's URL_DOCUMENT_MAPPING
    """
    dm = {}
    for type_name in STRING_TYPE, INT_TYPE, BOOLEAN_TYPE, FLOAT_TYPE:
        for i in range(_EXTRACT_RESULT_COUNT):
            dm["extract.extract_%s_%i" % (type_name, i)] = {
                "type": type_name,
                "default_value": None,
                "settings": {
                    # LIST,
                }
            }
    return dm


class ExtractResultsStreamDef(StreamDefBase):
    FILE = 'urlextract'
    HEADERS = (
        ('id', int),  # url_id
        ('label', str),  # user-entered name
        ('es_field', str),  # ES label
        ('agg', str),  # list, first, count, exist
        ('cast', str),  # empty (s), s(tr), i(nt), b(ool), f(loat)
        ('rank', int),  # for lists; they come unordered!
        ('value', str)  # bool is '0'/'1'
    )

    URL_DOCUMENT_MAPPING = _generate_ers_document_mapping()

    def process_document(self, document, stream):
        url_id, label, es_field, agg, cast, rank, value = stream
        if cast:
            value = self._apply_cast(cast, value)

        if agg != "list":
            document["extract"][es_field] = value
        else:
            self._put_in_place(document["extract"], es_field, rank, value)

    @staticmethod
    def _apply_cast(cast, value):
        """
        Cast value according to cast.
        :param cast: Expected type ("" == str)
        :type cast: str
        :param value: String input value
        :type value: str
        :return:Casted value
        """
        if not cast or cast == 's':
            return value
        if cast == 'i':
            return int(value)
        if cast == 'b':
            return value == '1'  # or value[0].lower() in 'typo'
        if cast == 'f':
            return float(value)
        raise AssertionError("{} not in 'sibf'".format(cast))

    @staticmethod
    def _put_in_place(extract, es_field, rank, value):
        """
        Put value in extract[label] at the specified rank.
        If the array is too short, add some None.
        :param extract: document["extract"]
        :type extract: dict
        :param es_field: ES field name
        :type es_field: str
        :param rank: position
        :type rank: int
        :param value: what to put
        :type value:
        """
        if rank < 0:
            return
        if extract[es_field] is None:
            tmp = []
        elif not isinstance(extract[es_field], list):
            tmp = [extract[es_field]]
        else:
            tmp = extract[es_field]
        while len(tmp) <= rank:
            tmp.append(None)
        tmp[rank] = value
        extract[es_field] = tmp
