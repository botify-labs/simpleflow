from cdf.features.comparison.constants import (
    QualitativeDiffResult,
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
)
from cdf.core.metadata.dataformat import assemble_data_format
from cdf.features.comparison import logger


def compute_qualitative_diff(ref_value, new_value):
    """Compare qualitative diff fields' values

    :param ref_value: the previous(reference) value
    :param new_value: the current value
    :return: a diff status string
    """
    if ref_value is None:
        if new_value is None:
            # both None
            # impossible
            logger.warning('Both value is None in diff')
            return None
        else:
            # reference is None,  new is not None
            # appeared
            return QualitativeDiffResult.APPEARED
    else:
        if new_value is None:
            # reference is not None, new is None
            # disappeared
            return QualitativeDiffResult.DISAPPEARED
        else:
            # both not None
            # need compare
            if ref_value == new_value:
                return QualitativeDiffResult.EQUAL
            else:
                return QualitativeDiffResult.CHANGED


def compute_qualitative_diff_list(ref_value, new_value):
    if ref_value is None and new_value is None:
        return None

    len_ref = len(ref_value) if ref_value is not None else 0
    len_new = len(new_value) if new_value is not None else 0

    # empty -> non-empty => appear
    # non-empty -> empty =< disappear
    if len_new == 0 and len_ref > 0:
        return QualitativeDiffResult.DISAPPEARED
    if len_new > 0 and len_ref == 0:
        return QualitativeDiffResult.APPEARED

    # both empty list => equal
    if len_new == len_ref == 0:
        return QualitativeDiffResult.EQUAL

    # both non-empty
    ref_value = ref_value[0]
    new_value = new_value[0]

    return compute_qualitative_diff(ref_value, new_value)


def compute_quantitative_diff(ref_value, new_value):
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


def get_diff_strategy(data_format):
    """Generate diff strategy from url data format

    Two diffing strategies:
        qualitative: returns a state (EQUAL, CHANGED, DISAPPEAR, APPEAR)
        quantitative: returns the diff value

    No strategy will be generated if there's no diff markers in field's data
    format.

    :param data_format: url data format
    :return: diff strategy dict (field -> diff_func)
    """
    diff_strategy = {}
    for field, value in data_format.iteritems():
        # check `settings` for explicit diff strategy
        if 'settings' in value:
            settings = value['settings']
            if DIFF_QUANTITATIVE in settings:
                diff_strategy[field] = compute_quantitative_diff
            elif DIFF_QUALITATIVE in settings:
                if LIST in settings:
                    # special case: list field
                    diff_strategy[field] = compute_qualitative_diff_list
                else:
                    diff_strategy[field] = compute_qualitative_diff

        # no diff strategy for this field, reports
        if field not in diff_strategy or diff_strategy.get(field) is None:
            logger.warning('No diff strategy found for {}'.format(field))
            diff_strategy.pop(field, None)

    return diff_strategy


# Generated from url data format
DIFF_STRATEGY = get_diff_strategy(assemble_data_format())


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
        try:
            diff_result = diff_func(ref_value, new_value)
        except Exception:
            logger.error(
                "Exception during diff for field {}, values: {}, {}",
                field, ref_value, new_value
            )
            raise
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