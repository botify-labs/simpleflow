import copy

from cdf.core.metadata.constants import FIELD_RIGHTS
from cdf.metadata.url.url_metadata import BOOLEAN_TYPE


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


def get_comparison_data_format(data_format, extras=EXTRA_FIELDS_FORMAT):
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