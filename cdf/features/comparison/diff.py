from cdf.features.comparison.constants import (
    CHANGED,
    EQUAL,
    DISAPPEARED,
    APPEARED,
    MatchingState
)
from cdf.utils.dict import (
    path_in_dict,
    get_subdict_from_path,
    update_path_in_dict
)
from cdf.metadata.url.url_metadata import (
    LIST,
    DIFF_QUALITATIVE,
    DIFF_QUANTITATIVE,
    STRING_TYPE,
    ES_DOC_VALUE
)
from cdf.utils.features import get_urls_data_format_definition


def get_diff_strategy(data_format):
    """Generate diff strategy from url data format

    :param data_format: url data format
    :return: diff strategy dict (field -> diff_func)
    """
    diff_strategy = {}
    for field, value in data_format.iteritems():
        if 'settings' in value:
            settings = value['settings']
            if DIFF_QUANTITATIVE in settings:
                diff_strategy[field] = quantitative_diff
            elif DIFF_QUALITATIVE in settings:
                if LIST in settings:
                    diff_strategy[field] = qualitative_diff_list
                else:
                    diff_strategy[field] = qualitative_diff
    return diff_strategy


def get_diff_data_format(data_format):
    """Generate the diff sub-document's data format

    The result should be used in the final mapping generation.
    Fields are not prefixed, should be prefixed if needed in
    mapping generation.

    :param data_format: url data format
    :return: diff sub-document's data format
    """
    diff_mapping = {}
    for field, value in data_format.iteritems():
        if 'settings' in value:
            settings = value['settings']
            if DIFF_QUALITATIVE in settings:
                diff_mapping[field] = {'type': STRING_TYPE}
            elif DIFF_QUANTITATIVE in settings:
                field_type = value['type']
                mapping = {'type': field_type}
                # also add doc_value flag, if it's present for
                # the original field
                if ES_DOC_VALUE in settings:
                    mapping['settings'] = {ES_DOC_VALUE}
                diff_mapping[field] = mapping
    return diff_mapping


def qualitative_diff(ref_value, new_value):
    """Compare qualitative diff fields' values

    :param ref_value: the previous(reference) value
    :param new_value: the current value
    :return: a diff status string
    """
    if ref_value is None:
        if new_value is None:
            # both None
            # impossible
            return None
        else:
            # reference is None,  new is not None
            # appeared
            return APPEARED
    else:
        if new_value is None:
            # reference is not None, new is None
            # disappeared
            return DISAPPEARED
        else:
            # both not None
            # need compare
            if ref_value == new_value:
                return EQUAL
            else:
                return CHANGED


def qualitative_diff_list(ref_value, new_value):
    if ref_value is None and new_value is None:
        return None

    len_ref = len(ref_value) if ref_value is not None else 0
    len_new = len(new_value) if new_value is not None else 0

    # empty -> non-empty => appear
    # non-empty -> empty =< disappear
    if len_new == 0 and len_ref > 0:
        return DISAPPEARED
    if len_new > 0 and len_ref == 0:
        return APPEARED

    # both empty list => equal
    if len_new == len_ref == 0:
        return EQUAL

    # both non-empty
    ref_value = ref_value[0]
    new_value = new_value[0]

    return qualitative_diff(ref_value, new_value)


def quantitative_diff(ref_value, new_value):
    """Calculate quantitative diff

    The difference is always calculated by:
        `ref_value` - `new_value`

    :param ref_value: the previous(reference) value
    :param new_value: the current value
    :return: the numerical difference
    """
    if ref_value is None or new_value is None:
        return None
    return ref_value - new_value


# Generated from url data format
DIFF_STRATEGY = get_diff_strategy(get_urls_data_format_definition())


# TODO should use an object-oriented style
# FieldDiffer
#   | QualitativeFieldDiffer
#   | QuantitativeFieldDiffer
#
# And this two different differ strategy get mixed-in
# into field definition object

# Since we're currently using a dictionary-oriented programming
# style, the diff impl sticks to it for the moment

def document_diff(ref_doc, new_doc, diff_strategy=DIFF_STRATEGY):
    """Diff two url documents

    :param ref_doc: reference url document
    :type ref_doc: dict
    :param new_doc: new url document
    :type new_doc: dict
    :param diff_strategy: a field -> diff_function mapping
    :type diff_strategy: dict
    :return: a diff document
    """
    diff = None
    for field, diff_func in diff_strategy.iteritems():
        ref_value = (get_subdict_from_path(field, ref_doc)
                     if path_in_dict(field, ref_doc) else None)
        new_value = (get_subdict_from_path(field, new_doc)
                     if path_in_dict(field, new_doc) else None)
        diff_result = diff_func(ref_value, new_value)
        if diff_result is not None:
            if diff is None:
                # lazily create diff document
                diff = {}
            update_path_in_dict(field, diff_result, diff)
    return diff


def diff(matched_doc_stream, diff_strategy=DIFF_STRATEGY):
    """Diff matched document stream

    :param matched_doc_stream: matched documents stream
    :type matched_doc_stream: stream of (MatchingState, (dict, dict))
    :return: matched_doc_stream along with a diff dict
    :rtype: (MatchingState, (dict, dict, dict))
    """
    for state, (ref_doc, new_doc) in matched_doc_stream:
        if state is MatchingState.MATCH:
            diff_doc = document_diff(ref_doc, new_doc, diff_strategy)
            yield state, (ref_doc, new_doc, diff_doc)
        else:
            yield state, (ref_doc, new_doc, None)