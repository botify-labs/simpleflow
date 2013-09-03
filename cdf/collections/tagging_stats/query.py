import os
import re
import operator
from pandas import HDFStore
import numpy

from cdf.collections.tagging_stats.constants import CROSS_PROPERTIES_COLUMNS, COUNTERS_FIELDS

from cdf.utils.s3 import fetch_files
from cdf.utils.dict import deep_dict, deep_update
from .utils import field_has_children, children_from_field


def is_dict_filter(filter_dict):
    """
    Check if the incoming dict is a filter (means "field" et "value" keys are set
    """
    return 'field' in filter_dict and 'value' in filter_dict


def is_boolean_operation_filter(filter_dict):
    return len(filter_dict) == 1 and filter_dict.keys()[0].lower() in ('and', 'or')


def std_type(value):
    if type(value) == numpy.bool_:
        return bool(value)
    elif type(value) == numpy.int64:
        return int(value)
    elif type(value) == numpy.float:
        if numpy.isnan(value):
            return 0
        else:
            return int(value)

    return value


class BadRequestException(Exception):
    pass


def transform_std_type(field, df_values):
    return std_type(df_values[field] if field in df_values else 0)


class MetricsQuery(object):

    BadRequestException = BadRequestException

    DISTRIBUTION_COLUMNS = CROSS_PROPERTIES_COLUMNS
    FIELDS = CROSS_PROPERTIES_COLUMNS + COUNTERS_FIELDS

    def __init__(self, df):
        self.df = df

    @classmethod
    def from_s3_uri(cls, crawl_id, rev_num, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
        # Fetch locally the files from S3
        tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
        files_fetched = fetch_files(s3_uri, tmp_dir, regexp='properties_stats_rev%d.h5' % rev_num, force_fetch=force_fetch)
        store = HDFStore(files_fetched[0][0])
        return cls(store['counter'])

    def get_func_from_filter_dict(self, df, _filter):
        # Not operator
        if _filter.get('not', False):
            _op = lambda i: operator.__not__(i)
        else:
            _op = lambda i: i

        predicate = _filter.get('predicate', None)
        if not predicate:
            if isinstance(_filter['value'], list):
                predicate = "in"
            else:
                predicate = "eq"

        if predicate == "eq":
            _predicate_func = lambda value, i: value == i
        elif predicate == "re":
            _predicate_func = lambda value, i: bool(re.search(value, i))
        elif predicate == "starts":
            _predicate_func = lambda value, i: i.startswith(value)
        elif predicate == "ends":
            _predicate_func = lambda value, i: i.endswith(value)
        elif predicate == "gte":
            _predicate_func = lambda value, i: i >= value
        elif predicate == "lte":
            _predicate_func = lambda value, i: i <= value
        elif predicate == "gt":
            _predicate_func = lambda value, i: i > value
        elif predicate == "lt":
            _predicate_func = lambda value, i: i < value
        elif predicate == "contains":
            _predicate_func = lambda value, i: value in i
        elif predicate == "in":
            _predicate_func = lambda value, i: any(i == v for v in value)

        return df[_filter['field']].map(lambda i: _op(_predicate_func(_filter['value'], i)))

    def _apply_filters_list(self, df, lst, _operator='or'):
        filters_func = None
        for _filter in lst:
            if filters_func is None:
                filters_func = self.get_func_from_filter_dict(df, _filter)
            else:
                getattr(operator, '__i%s__' % _operator)(filters_func, self.get_func_from_filter_dict(df, _filter))
        return filters_func

    def _apply_filters(self, df, filters, parent_operator="or"):
        filters_func = None
        if isinstance(filters, list):
            if filters[0].keys()[0].lower() in ('and', 'or'):
                _sub_operator_key = filters[0].keys()[0]
                filters_func = self._apply_filters_list(df, filters[0][_sub_operator_key], _operator=_sub_operator_key.lower())
            else:
                filters_func = self._apply_filters_list(df, filters, _operator=parent_operator)
        elif is_dict_filter(filters):
            return self.get_func_from_filter_dict(df, filters)
        elif is_boolean_operation_filter(filters):
            key = filters.keys()[0]
            _operator = key.lower()

            for _filter in filters[key]:
                if filters_func is None:
                    filters_func = self._apply_filters(df, _filter, parent_operator=_operator)
                else:
                    getattr(operator, "__i%s__" % _operator)(filters_func, self._apply_filters(df, _filter, parent_operator=_operator))
        else:
            raise Exception('Filter not well formated : %s' % filters)
        return filters_func

    def query(self, settings):
        """
        Return the total sum from a list of `fields` aggregated by cross-property

        :param settings

        Return a list of dictionaries with two keys "properties" and "counters".

        Ex :

        [
            {
                "properties": {
                    "host": "www.site.com",
                    "content_type": "text/html"
                },
                "counters": {
                    "pages_nb": 10,
                }
            },
            {
                "properties": {
                    "host": "subdomain.site.com",
                    "content_type": "text/html"
                },
                "counters": {
                    "pages_nb": 20
                }
            }
        ]
        """
        if 'fields' in settings:
            fields = settings['fields']
        else:
            fields = filter(lambda i: i not in self.DISTRIBUTION_COLUMNS, self.df.columns.tolist())

        final_fields = []
        for f in fields:
            if f not in self.FIELDS:
                raise self.BadRequestException('Field {} not allowed in query'.format(f))
            if field_has_children(f):
                final_fields += children_from_field(f)
            else:
                final_fields.append(f)

        results = {}

        df = self.df.copy()

        if 'filters' in settings:
            df = df[self._apply_filters(df, settings['filters'])]

        if 'group_by' in settings:
            df = self._map_host_group_by(df, settings['group_by'])
            df = self._map_resource_type_group_by(df, settings['group_by'])
            df = df.groupby(settings['group_by']).agg('sum').reset_index()
        else:
            """
            No group_by, we return a dictionnary with all counters
            """
            df = df.sum().reset_index()
            results = {}
            values = dict(df.values)
            for field in final_fields:
                deep_update(results, deep_dict({field: transform_std_type(field, values)}))
            return {"counters": results}

        results = []
        for i, n in enumerate(df.values):
            values = dict(zip(df.columns, n))
            counters = {}
            for field in final_fields:
                deep_update(counters, deep_dict({field: transform_std_type(field, values)}))

            result = {
                'properties': {field_: std_type(df[field_][i]) for field_ in settings['group_by']},
                'counters': counters
            }
            results.append(result)
        return results

    def _map_host_group_by(self, df, group_by):
        hosts = filter(lambda i: i.startswith('host__level'), group_by)
        if len(hosts) == 1:
            try:
                hostname = hosts[0]
                level = int(hostname[len('host__level')])
            except:
                raise Exception("Level has to be an integer (ex host__level1)")
            group_by.remove(hostname)
            group_by.append('host')

            def host_to_level(host, level):
                splited = host.rsplit('.', level)
                if len(splited) > level:
                    return '*.' + '.'.join(splited[-level:])
                return '.'.join(splited[-level:])

            df['host'] = df['host'].map(lambda name: host_to_level(name, level))
        elif len(hosts) > 1:
            raise Exception("It is not allowed to make a group_by with differents host's levels")
        return df

    def _map_resource_type_group_by(self, df, group_by):
        rt_ = filter(lambda i: i.startswith('resource_type__level'), group_by)
        if len(rt_) == 1:
            try:
                resource_type = rt_[0]
                level = int(resource_type[len('resource_type__level')])
            except:
                raise Exception("Level has to be an integer (ex resource_type__level1)")
            group_by.remove(resource_type)
            group_by.append('resource_type')

            def rename_resource_type(name, level):
                splited = name.split('/', level)
                if len(splited) > level:
                    return '/'.join(splited[:level]) + '/*'
                return name

            df['resource_type'] = df['resource_type'].map(lambda name: rename_resource_type(name, level))
        elif len(rt_) > 1:
            raise Exception("It is not allowed to make a group_by with differents resourc_type's levels")
        return df
