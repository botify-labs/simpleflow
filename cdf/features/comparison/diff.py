from cdf.features.comparison.constants import (
    CHANGED,
    EQUAL,
    DISAPPEARED,
    APPEARED
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
            raise Exception('Impossible diff case: both None')
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
    diff = {}
    for field, diff_func in diff_strategy.iteritems():
        ref_value = (get_subdict_from_path(field, ref_doc)
                     if path_in_dict(field, ref_doc) else None)
        new_value = (get_subdict_from_path(field, new_doc)
                     if path_in_dict(field, new_doc) else None)
        if not ref_value is None or not new_value is None:
            update_path_in_dict(
                field, diff_func(ref_value, new_value), diff)
    return diff


def diff(ref_doc_stream, new_doc_stream):
    """Diff two streams of documents

    :param ref_doc_stream: the reference crawl's document stream, the stream
        should be sorted on the `url` string of each document
    :param new_doc_stream: the new crawl's document stream, the stream should
        be sorted sorted on the `url` string of each document
    :return:
    """
    pass