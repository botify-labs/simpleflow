from cdf.query.constants import RENDERING, FIELD_RIGHTS
from cdf.metadata.url.url_metadata import LIST, ES_NO_INDEX
from cdf.core.features import Feature, generate_data_format

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
    rights = []
    for field_right in FIELD_RIGHTS:
        if field_right in settings:
            rights.append(field_right.value)
    if not rights:
        rights = [FIELD_RIGHTS.FILTERS.value, FIELD_RIGHTS.SELECT.value]

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


def get_fields(feature_options, remove_private=True,
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
    """
    # TODO(darkjh) create "Diff {}" groups for previous diff
    fields = []
    data_format = generate_data_format(
        feature_options, available_features=available_features)
    for field, config in data_format.iteritems():
        fields.append(_render_field(field, config))

    # now every elem of data model has a `group`
    # the returned result should be sorted on it
    # fields with empty group is guaranteed to be invisible (private)
    fields.sort(key=lambda m: m['group'])

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
