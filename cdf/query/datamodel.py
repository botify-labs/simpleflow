from cdf.query.constants import FLAG_STRING_URL
from cdf.metadata.url.url_metadata import LIST, ES_NO_INDEX
from cdf.core.features import Feature


def _render_field_to_end_user(stream_def, field):
    field_conf = stream_def.URL_DOCUMENT_MAPPING[field]
    settings = field_conf.get("settings", [])
    group = field_conf.get("group", getattr(stream_def, 'URL_DOCUMENT_DEFAULT_GROUP', ''))

    if FLAG_STRING_URL in settings:
        field_type = "url"
    else:
        field_type = field_conf["type"]

    return {
        "name": field_conf.get("verbose_name", ""),
        "value": field,
        "type": field_type,
        "is_sortable": True,
        "group": group,
        "multiple": LIST in settings,
        "searchable": ES_NO_INDEX not in settings
    }


def get_document_fields_from_features_options(features_options):
    fields = []
    for feature in Feature.get_features():
        if not feature.identifier in features_options:
            continue
        fields += feature.get_document_fields_from_options(features_options[feature.identifier])
    return fields


def get_end_user_document_fields_from_features_options(features_options):
    fields = []
    for field, stream_def in get_document_fields_from_features_options(features_options):
        fields.append(_render_field_to_end_user(field, stream_def))
    return fields
