# -*- coding: utf-8 -*-
import os
import re
import copy
import operator
from pandas import HDFStore
import numpy

from cdf.collections.suggestions.constants import CROSS_PROPERTIES_COLUMNS, COUNTERS_FIELDS

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


class BaseMetricsQuery(object):

    BadRequestException = BadRequestException

    DISTRIBUTION_COLUMNS = CROSS_PROPERTIES_COLUMNS
    FIELDS = CROSS_PROPERTIES_COLUMNS + COUNTERS_FIELDS

    def __init__(self, hdfstore, options=None):
        self.hdfstore = hdfstore
        self.df = self.hdfstore[self.DF_KEY]
        self.options = options

    @classmethod
    def from_s3_uri(cls, crawl_id, s3_uri, options=None, tmp_dir_prefix='/tmp', force_fetch=False):
        # Fetch locally the files from S3
        tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
        files_fetched = fetch_files(s3_uri, tmp_dir, regexp='suggest.h5', force_fetch=force_fetch)
        store = HDFStore(files_fetched[0][0])
        return cls(store, options)

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

    def get_fields_from_settings(self, settings):
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
        return final_fields

    def query(self, settings):
        """
        Return the total sum from a list of `fields` aggregated by cross-property

        :param settings

        Return a list of dictionaries with two keys "properties" and "counters".

        Ex :

        [
            {
                "properties": {
                    "depth": 1,
                    "http_code": 200
                },
                "counters": {
                    "pages_nb": 10,
                }
            },
            {
                "properties": {
                    "depth": 1,
                    "http_code": 301
                },
                "counters": {
                    "pages_nb": 20
                }
            }
        ]
        """
        final_fields = self.get_fields_from_settings(settings)

        results = {}
        df = self.df.copy()

        if 'filters' in settings:
            df = df[self._apply_filters(df, settings['filters'])]

        if 'group_by' in settings:
            df = df.groupby(settings['group_by']).agg('sum').reset_index()
            df = self.df_filter_after_agg(df)
            if 'sort' in settings:
                df.sort(columns=[k[0] for k in settings['sort']], ascending=[k[1] == "ASC" for k in settings['sort']], inplace=True)

        else:
            """
            No group_by, we return a dictionnary with all counters
            """
            df = df.sum().reset_index()
            df = self.df_filter_after_agg(df)
            if 'sort' in settings:
                df.sort(columns=[k[0] for k in settings['sort']], ascending=[k[1] == "ASC" for k in settings['sort']], inplace=True)
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
                'properties': {field_: self._display_field(field_, values[field_]) for field_ in settings['group_by']},
                'counters': counters
            }
            results.append(result)
        return results

    def df_filter_after_agg(self, df):
        """
        Filter the dataframe after aggregation if necessary
        """
        return df

    def _display_field(self, field, value):
        return std_type(value)


class MetricsQuery(BaseMetricsQuery):
    DF_KEY = "full_crawl"


class SuggestQuery(BaseMetricsQuery):
    DF_KEY = "suggest"

    def query_hash_to_string(self, value):
        ids = [int(v) for v in value.split(';')]
        return ' AND '.join([unicode(self.hdfstore['requests'][hash_id], "utf8") for hash_id in ids])

    """
    def _display_field(self, field, value):
        if field == "query":
            return self.query_hash_to_string(value)
        return super(SuggestQuery, self)._display_field(field, value)
    """

    def query(self, settings):
        final_fields = self.get_fields_from_settings(settings)
        df = self.df.copy()

        target_field = settings.get('target_field', 'pages_nb')

        if 'filters' in settings:
            df = df[self._apply_filters(df, settings['filters'])]

        df = df.groupby(['query']).agg('sum').reset_index()
        df.sort(columns=[target_field], ascending=[0], inplace=True)

        results = []
        for i, n in enumerate(df.values):
            values = dict(zip(df.columns, n))
            #counters = {}
            #for field in final_fields:
            #    deep_update(counters, deep_dict({field: transform_std_type(field, values)}))
            result = {
                'query': values['query'],
                'counters': {field: transform_std_type(field, values) for field in final_fields}
            }
            results.append(result)

        results = self.remove_results_with_common_hashes(settings, results)

        # Resolve query
        for i, r in enumerate(results):
            results[i]["query"] = self.query_hash_to_string(results[i]["query"])
            results[i]["counters"] = deep_dict(results[i]["counters"])
            if "children" in results[i]:
                if not settings.get('display_children', True):
                    del results[i]["children"]
                    continue
                results[i]["children"] = results[i]["children"][0:10]
                for k, c in enumerate(results[i]["children"]):
                    results[i]["children"][k]["query"] = self.query_hash_to_string(results[i]["children"][k]["query"])
                    results[i]["children"][k]["counters"] = deep_dict(results[i]["children"][k]["counters"])
        return results[0:30]

    def remove_results_with_common_hashes(self, settings, results):
        """
        If 1;3 and 1;2;3 has the same value for target field, we remove the one with the less hashes
        """
        target_field = settings.get('target_field', 'pages_nb')
        results_to_remove = set()
        for i, result in enumerate(results):
            hashes = result["query"].split(';')
            for _r in results:
                local_hashes = _r["query"].split(';')
                if all(h in local_hashes for h in hashes):
                    if result["counters"][target_field] == _r["counters"][target_field]:
                        if len(local_hashes) > len(hashes):
                            #print 'remove', result, 'in favor of', _r["query"]
                            results_to_remove.add(result["query"])
                        else:
                            #print 'remove', _r, 'in favor of', result["query"]
                            results_to_remove.add(_r["query"])
                    elif result["counters"][target_field] > _r["counters"][target_field] and _r["counters"][target_field] > 0:
                        if not "children" in results[i]:
                            results[i]["children"] = []
                        results[i]["children"].append(copy.copy(_r))
                        results_to_remove.add(_r["query"])
        for result in results:
            if result["query"] in results_to_remove:
                results.remove(result)
        return results

    def df_filter_after_agg(self, df):
        """
        if self.options['stats_urls_done']:
            # Take only urls > 3%
            threshold = int(float(self.options['stats_urls_done']) * 0.03)
            return df[df['pages_nb'] > threshold]
        """
        return df
