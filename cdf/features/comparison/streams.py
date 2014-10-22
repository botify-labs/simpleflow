import copy

from cdf.core.metadata.constants import FIELD_RIGHTS
from cdf.metadata.url.url_metadata import (
    BOOLEAN_TYPE,  STRING_TYPE,
    DIFF_QUANTITATIVE, DIFF_QUALITATIVE,
    ES_DOC_VALUE, ES_NOT_ANALYZED
)


# The document merge hack needs some extra flag fields
# Plus to this, we'll need to have a `previous` field
# which is a hard copy of the actual mapping
EXTRA_FIELDS_FORMAT = {
    'disappeared': {
        'type': BOOLEAN_TYPE,
        'default_value': None,
        'settings': {
            FIELD_RIGHTS.PRIVATE
        }
    },
    'previous_exists': {
        'type': BOOLEAN_TYPE,
        'default_value': None,
    }
}


def _transform_comparison_field(field):
    return 'previous.' + field


def _transform_comparison_config(config):
    config = copy.deepcopy(config)
    group_key = 'group'
    verbose_key = 'verbose_name'

    # make `Previous xxx` group
    group = config.get(group_key, '')
    if group is not '':
        group = 'previous.' + group
        config[group_key] = group

    # rename verbose name
    if verbose_key in config:
        config[verbose_key] = 'Previous {}'.format(config[verbose_key])
    return config


def _transform_diff_config(config, group=None, verbose_name=None):
    if group is not None:
        config['group'] = 'diff.{}'.format(group)

    if verbose_name is not None:
        config['verbose_name'] = 'Diff {}'.format(verbose_name)

    return config


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
        f = 'diff.' + field
        group = value.get('group', None)
        verbose_name = value.get('verbose_name', None)
        if 'settings' in value:
            settings = value['settings']
            if DIFF_QUALITATIVE in settings:
                mapping = {
                    'type': STRING_TYPE,
                    'settings': {
                        ES_NOT_ANALYZED
                    }
                }
                diff_mapping[f] = _transform_diff_config(
                    mapping, group, verbose_name)
            elif DIFF_QUANTITATIVE in settings:
                field_type = value['type']
                mapping = {
                    'type': field_type
                }
                # also add doc_value flag, if it's present for
                # the original field
                if ES_DOC_VALUE in settings:
                    mapping['settings'] = {ES_DOC_VALUE}
                diff_mapping[f] = _transform_diff_config(
                    mapping, group, verbose_name)

    return diff_mapping


def get_previous_data_format(data_format, extras=EXTRA_FIELDS_FORMAT):
    """Prepare internal data format for comparison feature

    Create `previous` fields.
    Also need to create new groups
        1. `Previous.xxx` group

    :param data_format: original internal data format
    :param extras: extra fields to be added for comparison feature
    :return: comparison's additional data format, to be merged with
        original data format
    """
    previous_format = {
        _transform_comparison_field(k): _transform_comparison_config(v)
        for k, v in data_format.iteritems()
    }
    previous_format.update(extras)

    return previous_format