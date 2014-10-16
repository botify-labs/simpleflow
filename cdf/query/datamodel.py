from cdf.core.metadata.constants import RENDERING, FIELD_RIGHTS
from cdf.metadata.url.url_metadata import LIST, ES_NO_INDEX
from cdf.core.features import Feature
from cdf.core.metadata.dataformat import generate_data_format

__all__ = ['get_fields', 'get_groups']


def _render_field(field, field_config):
    """Front-end friendly renders a field's configs

    :param field_config: configs of a field in data format
    :type field_config: dict
    :return: front-end friendly data model element
    :rtype: dict
    """
    settings = field_config.get('settings', set())
    group = field_config.get('group', '')
    field_type = field_config['type']
    data_type = field_config['type']

    if RENDERING.URL in settings:
        data_type = 'string'

    for flag in RENDERING:
        if flag in settings:
            field_type = flag.value
            break
    rights = _get_field_rights(settings)

    return {
        "name": field_config.get("verbose_name", ""),
        "value": field,
        "data_type": data_type,
        "field_type": field_type,
        "is_sortable": True,
        "group": group,
        "multiple": LIST in settings,
        "rights": rights
    }


def _get_field_rights(settings):
    """Compute field rights from the field settings.
    In standard cases, the function just select rights from the settings and
    add them to a list.
    However if no right is set (or only the ADMIN right),
    the "select" and "filters" rights are returned as default values.
    :param settings: the field settings
    :type settings: set
    :returns: list - a list of strings representing the rights.
    """
    result = []
    for field_right in FIELD_RIGHTS:
        if field_right in settings:
            result.append(field_right.value)
    # Set default value
    if not result or result == [FIELD_RIGHTS.ADMIN.value]:
        result.extend([FIELD_RIGHTS.FILTERS.value, FIELD_RIGHTS.SELECT.value])
    return result


def _check_visibility(config, visibility):
    return visibility in config.get("settings", [])


def _is_exists_fields(name):
    """Check `_exists` fields"""
    return name.endswith('_exists')


def _data_model_sort_key(elem):
    """A safe sort key function for data model"""
    _, config = elem
    group = config.get('group', '')
    order = config.get('order', 99999)
    return group, order


def get_fields(feature_options, remove_private=True, remove_admin=True,
               available_features=Feature.get_features()):
    """Returns a front-end friendly data model according to feature_options

    >>> get_fields({"main": None, "links": None})
    [
        {
            'value': 'depth',
            'data_type': 'integer',
            'field_type': 'time_sec'
            'name': 'Delay',
            'is_sortable': True,
            'group': 'metrics'
        },
        ...
    ]

    :param feature_options: feature options
    :param remove_private: should PRIVATE visibility fields be excluded
    :param remove_admin: should ADMIN visibility fields be excluded
    :param available_features: all available features
    """
    # TODO(darkjh) create "Diff {}" groups for previous diff
    data_format = generate_data_format(
        feature_options, available_features=available_features)

    # now every elem of data model has a `group`
    # the returned result should be sorted on it
    # fields with empty group is guaranteed to be invisible (private)
    fields = []
    for name, config in data_format.iteritems():
        # is it a `_exists` field ?
        is_exists = _is_exists_fields(name)
        # is it a private/admin field ?
        is_private = _check_visibility(config, FIELD_RIGHTS.PRIVATE)
        is_admin = _check_visibility(config, FIELD_RIGHTS.ADMIN)

        # do we remove this field b/c of private/admin ?
        is_private = remove_private and is_private
        is_admin = remove_admin and is_admin

        if not is_exists and not is_private and not is_admin:
            fields.append((name, config))

    # sort on group, then order within group
    fields.sort(key=_data_model_sort_key)

    # render data format to datamodel in place
    for i, (name, config) in enumerate(fields):
        fields[i] = _render_field(name, config)

    return fields


def get_groups(features_options):
    """
    Returns a list of allowed groups from enabled features
    Ex :
    [
        {"id": "main", "name": "Main"}
        ...
    ]
    """
    allowed_groups = set([f['group'] for f in get_fields(features_options)])
    groups = []
    for feature in Feature.get_features():
        for group in feature.groups:
            if group.name in allowed_groups:
                groups.append({'id': group.name, 'name': group.value})
    return groups
