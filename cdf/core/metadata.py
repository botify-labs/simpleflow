from cdf.query.constants import FIELD_RIGHTS
from cdf.core.features import Feature


def make_fields_private(mapping):
    """Make all the field of the mapping private.
    The initial intent of this function was to temporarly hide fields
    that we do not want to display to the user because they are still under
    development.
    If you definitely want to make a field private, it is recommanded to
    explicit add FIELD_RIGHTS.PRIVATE to its settings.
    :param mapping: input mapping as a dict field_name -> parameter dict
    :type mapping: dict
    :returns: dict - the modified mapping
    """
    for field in mapping.itervalues():
        if "settings" not in field:
            field["settings"] = set()

        #remove existing field rights
        settings = field["settings"]
        existing_field_rights = [
            elt for elt in settings if isinstance(elt, FIELD_RIGHTS)
        ]
        for field_right in existing_field_rights:
            settings.remove(field_right)

        #add private right
        settings.add(FIELD_RIGHTS.PRIVATE)
    return mapping


def _inject_feature_name(feature_name, data_format):
    """Inject feature name in each field's config"""
    for name, config in data_format.iteritems():
        config['feature'] = feature_name


def _inject_group(stream_def, data_format):
    """Inject the default group in each field's config"""
    default_group = getattr(
        stream_def,
        'URL_DOCUMENT_DEFAULT_GROUP',
        ''
    )
    group_key = 'group'
    for name, config in data_format.iteritems():
        if group_key not in config:
            config[group_key] = default_group


def _filter_field(field_value, feature_option):
    """Implicit contract between data format and feature option
        ex. for `lang` field
            - in data format:
                'lang': {
                    'enabled': lambda option: option is not None and option.get('lang', False)
                    ...
                }
            - in feature options
                {..., 'lang': True}
    """
    if 'enabled' in field_value:
        return field_value['enabled'](feature_option)
    else:
        return True


def _generate_data_format(feature_options, available_features):
    """Collect partial data formats from features, filter/transformed
    according to `feature_options`
    """
    result = {}
    activated_features = filter(
        lambda f: f.identifier in feature_options,
        available_features
    )

    # collect scattered data formats
    for f in activated_features:
        feature_name = f.identifier
        option = feature_options[feature_name]
        # TODO(darkjh) decouple stream_def with data format
        # it makes test very difficult
        # need to (mock data_format -> stream def -> feature)
        for stream_def in f.get_streams_def():
            if hasattr(stream_def, 'URL_DOCUMENT_MAPPING'):
                data_format = stream_def.URL_DOCUMENT_MAPPING
                data_format = {
                    k: v.copy()
                    for k, v in data_format.iteritems()
                    if _filter_field(v, option)
                }

                # inject information
                _inject_feature_name(feature_name, data_format)
                _inject_group(stream_def, data_format)

                result.update(data_format)

    return result


def generate_data_format(feature_options,
                         available_features=Feature.get_features()):
    """Collect partial data formats from features, filter/transformed
    according to `feature_options`

    It handles also `comparison` feature's data format

    :return: feature option specific data format
    :rtype: dict
    """
    # import special data format manipulation from `comparison` feature
    from cdf.features.comparison.tasks import get_comparison_data_format

    comparison_key = 'comparison'
    if comparison_key in feature_options:
        # comparison's manipulation
        # need to generate two data_format and put one inside another
        previous_options = feature_options[comparison_key]['options']
        del feature_options[comparison_key]
        previous_format = _generate_data_format(
            previous_options, available_features)
        previous_format = get_comparison_data_format(previous_format)
        data_format = _generate_data_format(feature_options, available_features)

        # merge the two data formats
        data_format.update(previous_format)
        return data_format
    else:
        # normal data format
        return _generate_data_format(feature_options, available_features)


def assemble_data_format():
    """Assemble partial data format from each feature

    :return: complete data format
    """
    urls_def = {}
    for f in Feature.get_features():
        for stream_def in f.get_streams_def():
            if hasattr(stream_def, 'URL_DOCUMENT_MAPPING'):
                urls_def.update(stream_def.URL_DOCUMENT_MAPPING)
    return urls_def