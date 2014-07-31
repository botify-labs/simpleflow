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


def get_diff_fields():
    pass


def get_diff_mapping():
    pass


def qualitative_diff(ref_value, new_value):
    """Compare qualitative diff fields' values

    :param ref_value: the previous(reference) value
    :param new_value: the current value
    :return: a diff status enum
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


DIFF_STRATEGY = {
    'canonical.in.url': qualitative_diff,
    'metadata.title.contents': qualitative_diff,
    'depth': quantitative_diff
}


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