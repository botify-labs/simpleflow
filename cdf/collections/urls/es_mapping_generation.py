from copy import deepcopy

from cdf.utils.dict import update_path_in_dict

_PROPERTY = 'properties'
_NO_INDEX = 'no_index'
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


def _parse_field_values(field_name, elem_values):
    """Parse field's settings into ES field configurations"""

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

    if elem_type == 'multi_field':
        return parse_multi_field(field_name, elem_values)
    elif elem_type == 'struct':
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
            'properties': fields_mapping
        }
    }

    if routing_field:
        # insert routing configuration
        es_mapping[doc_type]['_routing'] = {
            "required": True,
            "path": routing_field
        }

    return es_mapping