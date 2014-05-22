from cdf.query.constants import RENDERING
from cdf.metadata.url.url_metadata import LIST, ES_NO_INDEX
from cdf.core.features import Feature

__all__ = ['get_fields', 'get_groups']


def _render_field_to_end_user(stream_def, field):
    """
    Returns a user-friendly view of a field, that may be returned by the API
    Ex :
    >> {
    >>   "name": "Verbose name of the field",
    >>   "value": "field_name",
    >>   "data_type": "string",
    >>   "field_type": "url",
    >>   "group": "my_group",
    >>   "multiple": false,
    >>   "searchable": true
    >> }
    """
    field_conf = stream_def.URL_DOCUMENT_MAPPING[field]
    settings = field_conf.get("settings", [])
    group = field_conf.get("group", getattr(stream_def, 'URL_DOCUMENT_DEFAULT_GROUP', ''))

    field_type = field_conf["type"]
    data_type = field_conf["type"]
    if RENDERING.URL in settings:
        data_type = "string"
    for flag in (RENDERING.URL, RENDERING.TIME_SEC, RENDERING.TIME_MIN, RENDERING.PERCENT):
        if flag in settings:
            field_type = flag.value
            break

    return {
        "name": field_conf.get("verbose_name", ""),
        "value": field,
        "data_type": data_type,
        "field_type": field_type,
        "is_sortable": True,
        "group": group,
        "multiple": LIST in settings,
        "searchable": ES_NO_INDEX not in settings,
    }


def _get_document_fields_mapping_from_features_options(features_options, remove_private=True):
    """
    Returns a list of tuples (StreamDef, field) from StreamDef.URL_DOCUMENT_MAPPING matching with `features_options`
    """
    fields = []
    for feature in Feature.get_features():
        if not feature.identifier in features_options:
            continue
        for stream_def in feature.get_streams_def():
            stream_fields = [(stream_def, field) for field in stream_def.get_document_fields_from_options(features_options[feature.identifier], remove_private=True)]
            # Sort fields by order
            stream_fields = sorted(stream_fields, key=lambda k: k[0].URL_DOCUMENT_MAPPING[k[1]].get('order', None))
            fields += stream_fields
    return fields


def get_fields(features_options, remove_private=True):
    """
    Returns a list of allowed fields depending on enabled features and options
    Ex :
    >> get_fields({"main": None, "links": None})
    >> [
    >>     {'value': 'depth', 'data_type': 'integer', 'field_type': 'time_sec' 'name': 'Delay', 'is_sortable': True, 'group': 'metrics'}
    >>     ..
    >> ]
    """
    fields = []
    for entry in _get_document_fields_mapping_from_features_options(features_options, remove_private=True):
        stream_def, field = entry
        fields.append(_render_field_to_end_user(stream_def, field))
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
