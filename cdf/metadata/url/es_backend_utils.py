from copy import deepcopy

from cdf.utils.dict import update_path_in_dict
from cdf.core.metadata.dataformat import FIELD_RIGHTS
from .url_metadata import (STRING_TYPE, BOOLEAN_TYPE,
                           STRUCT_TYPE, MULTI_FIELD, LIST,
                           ES_NOT_ANALYZED, ES_NO_INDEX, LONG_TYPE,
                           INT_TYPE, ES_DOC_VALUE,
                           AGG_CATEGORICAL, AGG_NUMERICAL, FAKE_FIELD)


_PROPERTY = 'properties'
_SETTINGS = 'settings'


def _get_type(field_values):
    return field_values['type']


def _split_path(path):
    return path.split('.')


def _parse_field_path(path):
    """Parse a field path

    :return: parsed path
        ex. `a.b.c` will be parsed as `a.properties.b.properties.c`
        `properties` is a marker for ElasticSearch's object-type
    """
    return ('.' + _PROPERTY + '.').join(_split_path(path))


def _is_number_field(field_values):
    return _get_type(field_values) in (LONG_TYPE, INT_TYPE)


def _is_struct_field(field_values):
    return _get_type(field_values) == STRUCT_TYPE


def _is_boolean_field(field_values):
    return _get_type(field_values) == BOOLEAN_TYPE


def _is_string_field(field_values):
    return _get_type(field_values) == STRING_TYPE


def _is_list_field(field_values):
    return _SETTINGS in field_values and LIST in field_values[_SETTINGS]


def _is_multi_field(field_values):
    return _SETTINGS in field_values and MULTI_FIELD in field_values[_SETTINGS]


def _is_noindex_field(field_values):
    return _SETTINGS in field_values and ES_NO_INDEX in field_values[_SETTINGS]


def _is_categorical_field(field_values):
    return _SETTINGS in field_values and AGG_CATEGORICAL in field_values[_SETTINGS]


def _is_numerical_field(field_values):
    return _SETTINGS in field_values and AGG_NUMERICAL in field_values[_SETTINGS]


def _is_fake_field(field_values):
    return _SETTINGS in field_values and FAKE_FIELD in field_values[_SETTINGS]


def _parse_field_values(field_name, elem_values):
    """Parse field's settings into ElasticSearch field configurations"""

    def parse_struct_field(parsed_settings, values):
        values = deepcopy(values['values'])
        for inner_field in values:
            values[inner_field].update(parsed_settings)
        return {_PROPERTY: values}

    def parse_multi_field(parsed_settings, values):
        return parsed_settings

    def parse_simple_field(parsed_settings, values):
        sub_mapping = {'type': _get_type(values)}
        sub_mapping.update(parsed_settings)
        return sub_mapping

    def parse_settings(field_name, values):
        if 'settings' not in values:
            # return an empty dict for further processing
            # if there's no special setting options associated
            # with this field
            return {}

        settings = values['settings']
        parsed_settings = {}

        if ES_DOC_VALUE in settings:
            parsed_settings['fielddata'] = {
                'format': "doc_values"
            }

        if ES_NOT_ANALYZED in settings:
            parsed_settings['index'] = 'not_analyzed'
        elif ES_NO_INDEX in settings:
            parsed_settings['index'] = 'no'
        elif MULTI_FIELD in settings:
            field_type = _get_type(values)
            parsed_settings = {
                'type': 'multi_field',
                'fields': {
                    field_name: {'type': field_type},
                    'untouched': {
                        'type': field_type,
                        'index': 'not_analyzed'
                    }
                }
            }
        return parsed_settings

    parsed_settings = parse_settings(field_name, elem_values)

    if _is_fake_field(elem_values):
        return {}
    if _is_multi_field(elem_values):
        return parse_multi_field(parsed_settings, elem_values)
    elif _is_struct_field(elem_values):
        return parse_struct_field(parsed_settings, elem_values)
    else:  # if it's a simple field
        return parse_simple_field(parsed_settings, elem_values)


class DataBackend(object):
    """An abstract storage backend"""

    def __init__(self, data_format):
        """Constructor for Backend"""
        self.data_format = data_format

    # TODO abstract data format walk in the base class
    def query_fields(self):
        raise NotImplementedError()

    def select_fields(self):
        raise NotImplementedError()

    def list_fields(self):
        raise NotImplementedError()

    def field_default_value(self):
        raise NotImplementedError()

    def aggregation_fields(self):
        raise NotImplementedError()

    def mapping(self):
        raise NotImplementedError()


class ElasticSearchBackend(DataBackend):
    """ElasticSearch backend"""

    def __init__(self, data_format):
        super(ElasticSearchBackend, self).__init__(data_format)
        # cache
        self._select_fields = None
        self._query_fields = None
        self._list_fields = None
        self._field_default_value = None
        self._noindex_fields = None
        self._aggregation_fields = None
        self._categorical_agg_fields = None
        self._numerical_agg_fields = None
        self._mapping = None

    def __eq__(self, other):
        if not isinstance(other, ElasticSearchBackend):
            return False
        return self.data_format == other.data_format

    @classmethod
    def _routing(cls, routing_field):
        """Generate the routing setting"""
        return {
            "_routing": {
                "required": True,
                "path": routing_field
            }
        }

    @classmethod
    def _source(cls, excludes):
        """Generate the source control setting"""
        return {
            "_source": {
                "excludes": excludes
            }
        }

    @classmethod
    def _dynamic(cls, level):
        """Control dynamic mapping level

          - true (default):
                when an unknown field comes, the document is accepted and
                a default mapping will be created to index the field
          - false:
                when an unknown field comes, the document is accepted but
                will not create a default mapping, so the field remains only
                in `_source` and not indexed
          - strict:
                strict when an unknown field comes, the document is refused
        """
        return {
            "dynamic": level
        }

    @classmethod
    def index_settings(cls, nb_shards, nb_replicas, refresh):
        """Generate index level settings"""
        return {
            'settings': {
                'index': {
                    'number_of_shards': nb_shards,
                    'number_of_replicas': nb_replicas,
                    'refresh_interval': refresh,
                }
            }
        }

    def has_child(self, field):
        """Check if this field have child fields"""
        return any(i.startswith('{}.'.format(field)) for i in self.select_fields())

    def get_children(self, field):
        """Get all child fields of this field"""
        return filter(lambda i: i.startswith('{}.'.format(field)), self.select_fields())

    def query_fields(self):
        """Generate a lookup set for all complete fields

        Ex. `error_links.3xx.nb` but not a prefix like `error_links.3xx`
        """
        if self._query_fields:
            return self._query_fields

        lookup = set()

        for field_name, values in self.data_format.iteritems():
            splits = _split_path(field_name)
            for i, _ in enumerate(splits):
                lookup.add('.'.join(splits[:i + 1]))

        self._query_fields = lookup
        return self._query_fields

    def select_fields(self):
        """Generate a lookup set for query field membership check

        Ex. there's a `error_links.3xx.urls` record in data definition
            so valid fields will be:
                {`error_links`, `error_links.3xx`, `error_links.3xx.urls`}
        """
        if self._select_fields is None:
            lookup = set()

            for field_name, values in self.data_format.iteritems():
                splits = _split_path(field_name)
                for i, _ in enumerate(splits):
                    lookup.add('.'.join(splits[:i + 1]))
            self._select_fields = lookup
        return self._select_fields

    def field_default_value(self):
        """Generate a lookup for resolving the default value
        of a field

        :returns: a dict for (field_name, default_value) look up
        """
        BASIC_TYPE_DEFAULTS = {
            LONG_TYPE: 0,
            INT_TYPE: 0,
            STRING_TYPE: None,
            BOOLEAN_TYPE: False
        }

        def infer_for_basic_types(basic_type):
            return BASIC_TYPE_DEFAULTS.get(basic_type, None)

        if self._field_default_value is None:
            lookup = {}
            for path, values in self.data_format.iteritems():
                if 'default_value' in values:
                    if values['default_value'] is None:
                        # no default transform for this field
                        continue
                    # use user defined default value
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

                    # then infer from type
                    lookup[path] = infer_for_basic_types(values['type'])
            self._field_default_value = lookup
        return self._field_default_value

    def list_fields(self):
        """Generate a lookup table for list field from
        url data format definition

        :returns: a set for membership lookup
        """
        if self._list_fields is None:
            self._list_fields = {path for path, values in self.data_format.iteritems()
                                 if _is_list_field(values)}
        return self._list_fields

    def mapping(self, doc_type='urls', routing_field='crawl_id'):
        """Generate ES mapping from the intermediate format definition

        :param meta_mapping: internal intermediate format definition
        :param doc_type: default doc_type for the generated mapping
        :param routing_field: routing parameter in mapping
        :return: a valid ElasticSearch mapping
        """
        if self._mapping is None:
            fields_mapping = {}
            for path, value in self.data_format.iteritems():
                parsed_path = _parse_field_path(path)
                field_name = parsed_path.split('.')[-1]
                parsed_value = _parse_field_values(field_name, value)
                if parsed_value:
                    update_path_in_dict(parsed_path, parsed_value, fields_mapping)

            es_mapping = {
                doc_type: {
                    _PROPERTY: fields_mapping
                }
            }

            if routing_field:
                # insert routing configuration
                es_mapping[doc_type].update(
                    self._routing(routing_field))

            # excludes exists flags from `_source`
            es_mapping[doc_type].update(self._source(['*_exists']))

            # disallow dynamic mapping
            es_mapping[doc_type].update(self._dynamic('strict'))

            self._mapping = es_mapping

        return self._mapping

    def default_document(self, flatten=False):
        """Generate an json document for ElasticSearch with all field filled
        according to data format definition

        :param flatten: if True, generate document in a flatten manner
            eg. {'nested.field.flatten': None}
        :return: an empty json document
        """
        default_document = {}
        default_values = self.field_default_value()
        for path, _ in self.data_format.iteritems():
            default = default_values.get(path, None)
            if flatten:
                default_document[path] = default
            else:
                update_path_in_dict(path, default, default_document)

        return default_document

    def noindex_fields(self):
        """Generate a lookup table for list field from
        url data format definition

        :returns: a set for membership lookup
        """
        if self._noindex_fields is None:
            self._noindex_fields = {
                path for path, values in self.data_format.iteritems()
                if _is_noindex_field(values)}
        return self._noindex_fields

    def categorical_agg_fields(self):
        if self._categorical_agg_fields is None:
            self._categorical_agg_fields = {
                path for path, values in self.data_format.iteritems()
                if _is_categorical_field(values)}
        return self._categorical_agg_fields

    def numerical_agg_fields(self):
        if self._numerical_agg_fields is None:
            self._numerical_agg_fields = {
                path for path, values in self.data_format.iteritems()
                if _is_numerical_field(values)}
        return self._numerical_agg_fields

    def aggregation_fields(self):
        """Generate a lookup for all aggregation fields

        :return: a dict which contains 2 sub-dict
            {'categorical': {...}, 'numerical': {...}}
        """
        if self._aggregation_fields is None:
            result = {'categorical': set(),
                      'numerical': set()}

            for path, values in self.data_format.iteritems():
                if _is_numerical_field(values):
                    result['numerical'].add(path)
                if _is_categorical_field(values):
                    result['categorical'].add(path)
            self._aggregation_fields = result

        return self._aggregation_fields
