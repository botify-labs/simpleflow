import copy

from cdf.core.metadata.constants import FIELD_RIGHTS, RENDERING
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
    """Previous field should be prefix with `previous`"""
    return 'previous.' + field


def _transform_comparison_config(config):
    """Handle previous field's config's content"""
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


def _transform_diff_config(config, diff_kind):
    """Handle diff field's config's content"""
    diff_config = {}
    if diff_kind == DIFF_QUANTITATIVE:
        diff_config = copy.deepcopy(config)

        # remove diff related `settings`
        # keep all others
        settings = diff_config.get('settings', {})
        settings.discard(DIFF_QUALITATIVE)
        settings.discard(DIFF_QUANTITATIVE)
        # do not leave empty settings
        if len(settings) == 0:
            diff_config.pop('settings')

    elif diff_kind == DIFF_QUALITATIVE:
        # for qualitative diff
        #   - change type to STRING_TYPE
        #   - set to es:not_analyzed
        diff_config['type'] = STRING_TYPE
        diff_config['settings'] = {ES_NOT_ANALYZED}

    if 'group' in config:
        diff_config['group'] = 'diff.{}'.format(config['group'])

    if 'verbose_name' in config:
        diff_config['verbose_name'] = 'Diff {}'.format(config['verbose_name'])

    return diff_config


def get_diff_data_format(data_format):
    """Generate the diff sub-document's data format

    The result should be used in the final mapping generation.

    :param data_format: url data format
    :return: diff sub-document's data format
    """
    diff_mapping = {}
    for field, value in data_format.iteritems():
        f = 'diff.' + field

        if 'settings' not in value:
            continue

        settings = value['settings']

        if DIFF_QUALITATIVE in settings:
            diff_mapping[f] = _transform_diff_config(value, DIFF_QUALITATIVE)
        elif DIFF_QUANTITATIVE in settings:
            diff_mapping[f] = _transform_diff_config(value, DIFF_QUANTITATIVE)

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