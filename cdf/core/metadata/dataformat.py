import copy

from cdf.core.features import Feature
from cdf.core.metadata.constants import FIELD_RIGHTS
from cdf.features.comparison.streams import get_previous_data_format


def set_visibility(mapping, visibility):
    """Set all the field of the mapping a visibility level
    The initial intent of this function was to temporarily hide fields
    that we do not want to display to the user because they are still under
    development.
    If you definitely want to set the visibility level, it is recommended to
    explicit add FIELD_RIGHTS flags to its settings.
    :param mapping: input mapping as a dict field_name -> parameter dict
    :type mapping: dict
    :param visibility: visibility flag
    :type visibility: FIELD_RIGHTS
    :returns: dict - the modified mapping
    """
    if not isinstance(visibility, FIELD_RIGHTS):
        raise Exception("Wrong visibility config")

    for field in mapping.itervalues():
        if "settings" not in field:
            field["settings"] = set()
        field["settings"].add(visibility)
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


def check_enabled(enable_key):
    """Check if a field is enabled according to feature options

    It's expressed in field configuration and evaluated when generating
    feature option specific data format from feature options

    :param enable_key: field name
    :type enable_key: str
    :return: if the field is enabled
    :rtype: bool
    """
    def _check(opt):
        return opt is not None and opt.get(enable_key, False)
    return _check


def _is_field_enable(field_value, feature_option):
    """Implicit contract between data format and feature option
        ex. for `lang` field
            - in data format:
                'lang': {
                    'enabled': check_enabled('lang')
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
                    if _is_field_enable(v, option)
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
    # work with a copy is safer
    feature_options = copy.deepcopy(feature_options)
    comparison_key = 'comparison'
    if comparison_key in feature_options:
        # comparison's manipulation
        # need to generate two data_format and put one inside another
        previous_options = feature_options[comparison_key]['options']
        del feature_options[comparison_key]
        previous_format = _generate_data_format(
            previous_options, available_features)
        previous_format = get_previous_data_format(previous_format)
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


