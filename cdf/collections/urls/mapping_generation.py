from cdf.utils.dict import update_path_in_dict

_PROPERTY = 'properties'
_NO_INDEX = 'no_index'
_INCLUDE_NOT_ANALYZED = 'include_not_analyzed'
_LIST = 'list'
_NOT_ANALYZED = 'not_analyzed'


def _split_path(path):
    return path.split('.')


def _parse_field_path(path):
    """Parse a field path

    :return: parsed path
        ex. `a.b.c` will be parsed as `a.properties.b.properties.c`
        `properties` is a marker for ES's object-type
    """
    return ('.'+_PROPERTY+'.').join(_split_path(path))


def _parse_field_values(field_name, elem_vals):
    """Parse field's settings into ES field configurations"""

    es_field_settings = {}
    elem_type = elem_vals['type']

    if 'settings' in elem_vals:
        elem_settings = elem_vals['settings']
    else:
        # trivial case, no settings
        es_field_settings['type'] = elem_type
        return es_field_settings

    if _INCLUDE_NOT_ANALYZED in elem_settings:
        # use `multi_field` in this case
        es_field_settings['type'] = 'multi_field'
        es_field_settings['fields'] = {
            field_name: {
                'type': elem_type
            },
            # included an untouched field
            'untouched': {
                'type': elem_type,
                'index': 'not_analyzed'
            }
        }
    else:
        es_field_settings['type'] = elem_type
        if _NO_INDEX in elem_settings:
            es_field_settings['index'] = 'no'
        elif _NOT_ANALYZED in elem_settings:
            es_field_settings['index'] = 'not_analyzed'

    return es_field_settings


def generate_es_mapping(meta_mapping, routing_field=None):
    """Generate ES mapping from the intermediate format definition

    :param meta_mapping: internal intermediate format definition
    :param routing_field: routing parameter in mapping
    :return: a valid ES mapping
    """
    mapping = {}
    for path, value in meta_mapping.iteritems():
        parsed_path = _parse_field_path(path)
        field_name = parsed_path.split('.')[-1]
        parsed_value = _parse_field_values(field_name, value)
        update_path_in_dict(parsed_path, parsed_value, mapping)

    return mapping