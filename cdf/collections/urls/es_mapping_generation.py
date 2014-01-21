from copy import deepcopy

from cdf.utils.dict import update_path_in_dict


__ALL__ = ['generate_es_mapping',
           'generate_multi_field_lookup',
           'generate_default_value_lookup',
           'generate_list_field_lookup',
           'generate_valid_field_lookup']


_PROPERTY = 'properties'
_NO_INDEX = 'no_index'
_NOT_ANALYZED = 'not_analyzed'
_LIST = 'list'
_MULTI_FIELD_TYPE = 'multi_field'
_STRUCT_TYPE = 'struct'
_NUMBER_TYPE = 'long'
_STRING_TYPE = 'string'
_BOOLEAN_TYPE = 'boolean'


def _split_path(path):
    return path.split('.')


def _parse_field_path(path):
    """Parse a field path

    :return: parsed path
        ex. `a.b.c` will be parsed as `a.properties.b.properties.c`
        `properties` is a marker for ElaticSearch's object-type
    """
    return ('.'+_PROPERTY+'.').join(_split_path(path))


def _is_number_field(field_values):
    return field_values['type'] == _NUMBER_TYPE


def _is_struct_field(field_values):
    return field_values['type'] == _STRUCT_TYPE


def _is_boolean_field(field_values):
    return field_values['type'] == _BOOLEAN_TYPE


def _is_string_field(field_values):
    return field_values['type'] == _STRING_TYPE


def _is_list_field(field_values):
    return 'settings' in field_values and 'list' in field_values['settings']


def _is_multi_field(field_values):
    return field_values['type'] == _MULTI_FIELD_TYPE


def _parse_field_values(field_name, elem_values):
    """Parse field's settings into ElasticSearch field configurations"""

    def parse_struct_field(parsed_settings, values):
        values = deepcopy(values['values'])
        for inner_field in values:
            values[inner_field].update(parsed_settings)
        return {'properties': values}

    def parse_multi_field(field_name, values):
        field_type = values['field_type']
        return {
            'type': 'multi_field',
            'fields': {
                field_name: {'type': field_type},
                'untouched': {
                    'type': field_type,
                    'index': 'not_analyzed'
                }
            }
        }

    def parse_simple_field(parsed_settings, values):
        sub_mapping = {'type': values['type']}
        sub_mapping.update(parsed_settings)
        return sub_mapping

    def parse_settings(values):
        if 'settings' in values:
            settings = values['settings']
            parsed_settings = {}
            if _NOT_ANALYZED in settings:
                parsed_settings['index'] = 'not_analyzed'
            elif _NO_INDEX in settings:
                parsed_settings['index'] = 'no'
            return parsed_settings
        else:
            return {}

    elem_type = elem_values['type']
    parsed_settings = parse_settings(elem_values)

    if elem_type == _MULTI_FIELD_TYPE:
        return parse_multi_field(field_name, elem_values)
    elif elem_type == _STRUCT_TYPE:
        return parse_struct_field(parsed_settings, elem_values)
    else:
        return parse_simple_field(parsed_settings, elem_values)


def generate_es_mapping(meta_mapping,
                        doc_type='urls',
                        routing_field='crawl_id'):
    """Generate ES mapping from the intermediate format definition

    :param meta_mapping: internal intermediate format definition
    :param doc_type: default doc_type for the generated mapping
    :param routing_field: routing parameter in mapping
    :return: a valid ES mapping
    """
    fields_mapping = {}
    for path, value in meta_mapping.iteritems():
        parsed_path = _parse_field_path(path)
        field_name = parsed_path.split('.')[-1]
        parsed_value = _parse_field_values(field_name, value)
        update_path_in_dict(parsed_path, parsed_value, fields_mapping)

    es_mapping = {
        doc_type: {
            _PROPERTY: fields_mapping
        }
    }

    if routing_field:
        # insert routing configuration
        es_mapping[doc_type]['_routing'] = {
            "required": True,
            "path": routing_field
        }

    return es_mapping


def generate_multi_field_lookup(meta_mapping):
    """Generate a lookup table for multi_field field
    from url data format definition

    :returns: a set for membership lookup
    """
    return {path for path, values in meta_mapping.iteritems()
            if _is_multi_field(values)}


def generate_list_field_lookup(meta_mapping):
    """Generate a lookup table for list field from
    url data format definition

    :returns: a set for membership lookup
    """
    return {path for path, values in meta_mapping.iteritems()
            if _is_list_field(values)}


def generate_default_value_lookup(meta_mapping):
    """Generate a lookup for resolving the default value
    of a field

    :returns: a dict for (field_name, default_value) look up
    """
    BASIC_TYPE_DEFAULTS = {
        _NUMBER_TYPE: 0,
        _STRING_TYPE: None,
        _BOOLEAN_TYPE: False
    }

    def infer_for_basic_types(basic_type):
        try:
            default_value = BASIC_TYPE_DEFAULTS[basic_type]
        except KeyError:
            default_value = None
        return default_value

    lookup = {}
    for path, values in meta_mapping.iteritems():
        if 'default_value' in values:
            lookup[path] = values['default_value']
        else:
            # infer default value from field's type
            # order is IMPORTANT here
            #   a list of structs is considered a list, so
            #   it defaults to an empty list but not `None`

            # if a list field, defaults to empty list
            if _is_list_field(values):
                lookup[path] = []
                continue

            # if a struct field, defaults to `None`
            if _is_struct_field(values):
                lookup[path] = None
                continue

            # then infer from types
            if _is_multi_field(values):
                lookup[path] = infer_for_basic_types(values['field_type'])
            else:
                lookup[path] = infer_for_basic_types(values['type'])

    return lookup


def generate_valid_field_lookup(meta_mapping):
    """Generate a lookup set for query field membership check

    Ex. there's a `error_links.3xx.urls` record in data definition
        so valid fields will be:
            {`error_links`, `error_links.3xx`, `error_links.3xx.urls`}
    """
    lookup = set()

    for path in meta_mapping:
        splits = _split_path(path)
        for i, _ in enumerate(splits):
            lookup.add('.'.join(splits[:i+1]))

    return lookup