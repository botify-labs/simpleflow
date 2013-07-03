import os
import re
import operator
from pandas import HDFStore
import numpy

from cdf.collections.properties_stats.constants import CROSS_PROPERTIES_COLUMNS, CROSS_PROPERTIES_META_COLUMNS

from cdf.utils.s3 import fetch_files


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
    return value


class CounterRequest(object):

    def __init__(self, df):
        self.df = df

    @classmethod
    def from_s3_uri(cls, crawl_id, rev_num, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
        # Fetch locally the files from S3
        tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
        files_fetched = fetch_files(s3_uri, tmp_dir, regexp='properties_stats_rev%d.h5' % rev_num, force_fetch=force_fetch)
        store = HDFStore(files_fetched[0][0])
        return cls(store[cls.STORE_KEY])

    def _transform_filter_value(self, value):
        """
        This function is called to create filters from `resource_type` and `host`
        Wildcards (*) are replaced by a regular expression to make the check
        """
        pattern = "^%s$" % value.replace('*', '([\w-]+)')
        return pattern

    def get_func_from_filter_dict(self, df, _filter):
        if isinstance(_filter['value'], list):
            _clean_filters = map(lambda i: self._transform_filter_value(i), _filter['value'])
            return df[_filter['field']].map(lambda i: any(bool(re.search(_v, i)) for _v in _clean_filters))
        else:
            return df[_filter['field']].map(lambda i: bool(re.search(self._transform_filter_value(_filter['value']), i)))

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

    def fields_sum(self, settings):
        """
        Return the total sum from a list of `fields`


        :param settings
        :param resource_type : a list of resource types. wildcards (*) are allowed at the start or at the end of each resource_type
        :param _filters : a list of defined filters (coming from a parent class)

        Return a dict with the `field` as key and sum as value
        """
        if 'fields' in settings:
            fields = settings['fields']
        else:
            fields = self.df.columns()

        if 'filters' in settings:
            df = self.df.copy()
            df = df[self._apply_filters(df, settings['filters'])]
        else:
            df = self.df
        return {f: int(df[f].sum()) for f in settings['fields']}

    def fields_sum_by_property(self, settings):
        """
        Return the total sum from a list of `fields` aggregated by cross-property

        :param fields : a list of files
        :param resource_type : a list of resource types. wildcards (*) are allowed at the start or at the end of each resource_type
        :params group_by : if None, return all properties combinations possible. If a tuple of properties, return all combination matching those fields
        :param _filters : a list of defined filters (coming from a parent class)

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
        results = {}
        if 'group_by' in settings:
            group_by = settings['group_by']
        else:
            group_by = self.DISTRIBUTION_COLUMNS

        if 'fields' in settings:
            fields = settings['fields']
        else:
            fields = filter(lambda i: i not in self.DISTRIBUTION_COLUMNS, self.df.columns.tolist())

        df = self.df.copy()
        if 'filters' in settings:
            df = df[self._apply_filters(df, settings['filters'])]
        df = self._map_host_group_by(df, group_by)
        df = self._map_resource_type_group_by(df, group_by)
        df = df.groupby(group_by).agg('sum').reset_index()
        results = []
        for i, n in enumerate(df.values):
            result = {
                'properties': {field_: std_type(df[field_][i]) for field_ in group_by},
                'counters': {field_: int(df[field_][i]) if df[field_][i] > 0 else 0 for field_ in fields}
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


class PropertiesStatsRequest(CounterRequest):
    DISTRIBUTION_COLUMNS = CROSS_PROPERTIES_COLUMNS
    STORE_KEY = 'counter'


class PropertiesStatsMetaRequest(CounterRequest):
    DISTRIBUTION_COLUMNS = CROSS_PROPERTIES_META_COLUMNS
    STORE_KEY = 'meta_unicity'
