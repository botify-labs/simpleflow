# -*- coding: utf-8 -*-
import os
import re
import json
import copy
import operator
from pandas import HDFStore
import numpy
import itertools

from cdf.collections.suggestions.constants import CROSS_PROPERTIES_COLUMNS, COUNTERS_FIELDS

from cdf.utils.s3 import fetch_files, fetch_file
from cdf.utils.dict import deep_dict, deep_update, flatten_dict
from cdf.streams.utils import split_file
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

        if settings.get('target_field') == "score":
            fields.append('score')

        final_fields = []
        for f in fields:
            #if f not in self.FIELDS:
            #    raise self.BadRequestException('Field {} not allowed in query'.format(f))
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
        if isinstance(settings, list):
            return [self.query(s) for s in settings]

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
        for idx in df.index:
            values = {c: df[c][idx] for c in df.columns}
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


class MetricsPatternQuery(BaseMetricsQuery):
    """
    Allow the query to be grouped by a given pattern (maps to the `query` field)
    """
    DF_KEY = "suggest"


class SuggestQuery(BaseMetricsQuery):
    DF_KEY = "suggest"

    def __init__(self, hdfstore, options=None):
        super(SuggestQuery, self).__init__(hdfstore, options)

        if not 'children' in self.hdfstore.keys():
            self.child_relationship_set = set()
        else:
            child_frame = self.hdfstore['children']
            self.child_relationship_set = self.compute_child_relationship_set(child_frame)

    def query_hash_to_string(self, value):
        """get the full-letter query corresponding to a hash
        :param value: the hash
        :type value: int
        :returns: unicode - the full-letter query"""
        return unicode(self.hdfstore['requests'].ix[str(value), 'string'], "utf8")

    def query_hash_to_verbose_string(self, value):
        """get the full-letter verbose query corresponding to a hash
        :param value: the hash
        :type value: int
        :returns: unicode - the full-letter query"""
        return json.loads(unicode(self.hdfstore['requests'].ix[str(value), 'verbose_string'], "utf8"))

    def query(self, settings, sort_results=True):
        df = self.df.copy()
        return self._query(df, settings, sort_results)

    def _query(self, df, settings, sort_results):
        """The method that actually runs the query.
        The method is almost identical to the query() function
        but it is easier to test since we can pass the dataframe as parameter.
        :param df: the dataframe containing the aggregated data
                   sample columns are : query, depth, http_code, delay_lt_500ms
        :type df: pandas.DataFrame
        :param settings: a dictionary representing the query to run
        :type setting: dict
        :returns: list - the list of results corresponding to the query.
                         the list is sorted by descending "target_field" value.
                         each result is a dict with keys :
                         - "query_hash_id"
                         - "query",
                         - "query_bql",
                         - "score",
                         - "score_pattern"
                         - "percent_pattern"
                         - "percent_total"
                         - "counters"
        """
        results = self._raw_query(df, settings)

        #remove empty results
        target_field = settings.get('target_field', 'pages_nb')
        results = [result for result in results if
                   result["counters"][target_field] > 0]

        if len(results) == 0:
            return results

        if sort_results:
            results = self.sort_results_by_target_field(settings, results)
            results = self.remove_equivalent_parents(settings, results)
            results = self.hide_less_relevant_children(results)

        # Request Metrics query in order to get the total number of elements
        total_results = self._get_total_results(settings)
        total_results_by_pattern = self._get_total_results_by_pattern(settings)

        self._compute_scores(results, target_field,
                             total_results, total_results_by_pattern)
        self._resolve_results(results)
        return results[0:30]

    def _raw_query(self, df, settings):
        """Run a query on the dataframe,
        but does not perform any postprocessing on it: no result filtering,
                                                       no query resolution
        :param df: the dataframe containing the aggregated data
                   sample columns are : query, depth, http_code, delay_lt_500ms
        :type df: pandas.DataFrame
        :param settings: a dictionary representing the query to run
        :type setting: dict
        :returns: list - the list of results corresponding to the query.
                         the list is sorted by descending "target_field" value
                         each result is a dict with keys : "query", "counters"
                         with "query" the hash of the query and
                         "counters" a dict which keys are the fields requested
                         in the query.
        """
        target_field = settings.get('target_field', 'pages_nb')

        if 'filters' in settings:
            #explicitly select rows, [] notation is ambiguous
            #and may select columns
            df = df.loc[self._apply_filters(df, settings['filters'])]

        #if the dataframe is empty the result will be empty
        if len(df) == 0:
            return []

        df = df.groupby(['query']).agg('sum').reset_index()

        #If target field is {"div": [a, b]},
        #we create a new column on the current
        #dataframe that div a by b
        if isinstance(target_field, dict) and target_field.keys() == ["div"]:
            num_field = target_field["div"][0]  # numerator field
            den_field = target_field["div"][1]  # denominator field

            #multiply by 1.0 to convert to float
            #cf http://stackoverflow.com/questions/12183432/typecasting-before-division-or-any-other-mathematical-operator-of-columns-in-d
            df["score"] = df[num_field] * 1.0 / df[den_field]
            target_field = "score"
            settings["target_field"] = target_field

        df.sort(columns=[target_field], ascending=[0], inplace=True)

        final_fields = self.get_fields_from_settings(settings)

        results = []
        for i, n in enumerate(df.values):
            values = dict(zip(df.columns, n))
            result = {
                'query': values['query'],
                'counters': {field: transform_std_type(field, values) for
                             field in final_fields}
            }
            results.append(result)

        return results

    def _resolve_results(self, results):
        """Transform results identified by their hashes
        to a result identified by their full-letter queries
        :param results: the list of input results.
                        the results will be modified by the method
        :type results: list
        """
        # Resolve query
        for result in results:
            self._resolve_result(result)
            if "children" in result:
                result["children"] = result["children"][0:10]
                for child in result["children"]:
                    self._resolve_result(child)

    def _resolve_result(self, result):
        """Transform a result identified by its hash
        to a result identified by its full letter query
        :param result: the result to resolve.
                       It will be modified by the method
        :type result: dict
        """
        query_hash_id = int(result["query"])
        result["query_hash_id"] = query_hash_id
        result["query_bql"] = self.query_hash_to_string(query_hash_id)
        result["query"] = self.query_hash_to_verbose_string(query_hash_id)
        result["counters"] = deep_dict(result["counters"])

    def _compute_scores(self, results, target_field,
                        total_results, total_results_by_pattern):
        """Compute the different metrics for the results
        :param results: the list of results.
                        the results will be modified by the method
        :type results: list
        :param target_field: the target_field used in the query
        :type target_field: string
        :param total_results: the number of urls matching the query
        :type total_results: int
        :param total_results_by_pattern: the size of the patterns
                                         the patterns are identified by their hash
        :type total_results_by_pattern: dict
        """
        for result in results:
            query_hash_id = int(result["query"])
            pattern_size = total_results_by_pattern[query_hash_id]
            self._compute_scores_one_result(result, target_field,
                                            total_results, pattern_size)

    def _compute_scores_one_result(self, result, target_field,
                                   total_results, pattern_size):
        """Compute the different metrics for one result
        The method computes four metrics:
        - score: nb urls with the target_field property
        - score_pattern : nb urls in pattern
        - percent_pattern : proportion of urls with target_field property
                            in the pattern (= 100 * score/score_pattern)
        - percent_total : proportion of urls from the pattern
                          in the urls with the target_field property
                          (= 100 * score/nb_url_with_property)

        :param result: the input result.
                       It will be modified by the method
        :type results: dict
        :param target_field: the target_field used in the query
        :type target_field: string
        :param total_results: the number of urls matching the query
        :type total_results: int
        :param pattern_size: the size of the pattern corresponding to result
        :type pattern_size: int
        """
        result["score"] = result["counters"][target_field]
        # if total_results is zero, it must comes from a target_field
        # based on a complex operation like "div"
        # So we cannot know the value from the full crawl
        if total_results:
            result["percent_total"] = round(float(result["counters"][target_field]) * 100.00 / float(total_results), 1)
        else:
            result["percent_total"] = -1

        result["score_pattern"] = pattern_size
        result["percent_pattern"] = round(float(result["counters"][target_field]) * 100.00 / float(pattern_size), 1)

    def _get_total_results(self, query):
        """Return the total number of items for the given query
        :param query: the input query
        :type query: dict
        :returns: int
        """
        q = MetricsQuery(self.hdfstore)
        total_query = {
            "fields": [query["target_field"]]
        }
        if "filters" in query:
            total_query["filters"] = query["filters"]
        r = q.query(total_query)
        return flatten_dict(r["counters"])[query["target_field"]]

    def _get_total_results_by_pattern(self, query):
        """Return the total number of items for the given query
        :param query: the input query
        :type query: dict
        :returns: dict
        """
        q = MetricsPatternQuery(self.hdfstore)
        total_query = {
            "fields": ["pages_nb"],
            "group_by": ["query"]
        }
        r = q.query(total_query)
        return {int(v["properties"]["query"]): v["counters"]["pages_nb"] for v in r}

    def sort_results_by_target_field(self, settings, results):
        """Sort the query results by target field count.
        For instance if we look for elements with title not set:
        - pattern A has size 200 and contains 10 elements with h1 not set
        - pattern B has size 110 and contains 100 elements with h1 not set
        this method will place pattern B first.

        Sorting mode can be changed by adding a `target_sort` on settings
        with allowed values "asc" or "desc" (desc by default)

        :param settings: the input query
        :type settings: dict
        :param results: the list of results to sort.
                        each result is a dict
        :type results: list
        :returns: list
        """
        target_field = settings.get('target_field', 'pages_nb')
        target_sort = settings.get('target_sort', 'desc')
        reverse = target_sort == "desc"
        results = sorted(results,
                         reverse=reverse,
                         key=lambda x: x["counters"][target_field])
        return results

    def is_child(self, parent_hash, child_hash):
        """Test if a pattern is the child from an other pattern,
        given their two hashes.
        :param parent_hash: the hash of the potential parent pattern
        :type parent_hash: int
        :param child_hash: the hash of the potential parent pattern
        :type child_hash: int
        :returns: bool
        """
        return (parent_hash, child_hash) in self.child_relationship_set

    def compute_child_relationship_set(self, child_frame):
        """Build a set of tuples (parent_hash, child_hash)
        to be able to test fast if a relationship exists.
        :param child_frame : a pandas dataframe with two columns:
                            - parent : contains the parent pattern hash
                            - child : contains the parent pattern hash
                            Each row of the frame represents
                            a parent/child relationship
                            between two patterns.
        :type child_frame: pandas.DataFrame
        :returns: set
        """
        result = set()
        for count, row in child_frame.iterrows():
            parent_hash = row["parent"]
            child_hash = row["child"]
            result.add((parent_hash, child_hash))
        return result

    def remove_equivalent_parents(self, settings, results):
        """This method removes parent results if they have a child which
        contains the same number of relevant elements.

        For instance if we look for elements with title not set:
        - pattern A has size 200 and contains 100 elements with h1 not set
        - pattern B has size 110 and contains 100 elements with h1 not set

        pattern A is a parent of pattern B.

        Displaying pattern A to the user would not help him.
        pattern B is more relevant as it is more specific.

        The present method would remove pattern A from results

        :param settings: the query settings
        :type settings: dict
        :param results: the query results
        :type results: list
        :returns: list
        """

        target_field = settings.get('target_field', 'pages_nb')
        hashes_to_remove = []
        #It depends if we assume that parent always come first in the list
        for potential_parent, potential_child in itertools.permutations(results, 2):
            potential_parent_hash = potential_parent["query"]
            potential_child_hash = potential_child["query"]
            if self.is_child(potential_parent_hash, potential_child_hash):
                parent_target_field_count = potential_parent["counters"][target_field]
                child_target_field_count = potential_child["counters"][target_field]
                if parent_target_field_count == child_target_field_count:
                    hashes_to_remove.append(potential_parent_hash)

        results = [result for result in results if not result["query"] in hashes_to_remove]
        return results

    def hide_less_relevant_children(self, results):
        """Once we have displayed a node,
        displaying its children would confuse the user.
        The present method :
        - detects such children
        - remove them from the result
        - add them as children of their parent

        The method requires the input results to be sorted.
        The sort criterion does not matter.

        :param results: the list of results to process
        :type results: list
        :returns: list
        """
        hashes_to_remove = []
        for potential_parent, potential_child  in itertools.combinations(results, 2):
            potential_parent_hash = potential_parent["query"]
            potential_child_hash = potential_child["query"]

            if self.is_child(potential_parent_hash, potential_child_hash):
                hashes_to_remove.append(potential_child_hash)
                if not "children" in potential_parent:
                    potential_parent["children"] = []
                potential_parent["children"].append(copy.copy(potential_child))

        results = [result for result in results if
                   result["query"] not in hashes_to_remove]
        return results

    def df_filter_after_agg(self, df):
        """
        Does nothing
        :param df: the input dataframe
        :type df: pandas.DataFrame
        :returns: pandas.DataFrame
        """
        """
        if self.options['stats_urls_done']:
            # Take only urls > 3%
            threshold = int(float(self.options['stats_urls_done']) * 0.03)
            return df[df['pages_nb'] > threshold]
        """
        return df


class SuggestSummaryQuery(object):

    def __init__(self, content):
        self.content = content

    @classmethod
    def from_s3_uri(cls, crawl_id, s3_uri, options=None,
                    tmp_dir_prefix='/tmp', force_fetch=False):
        # Fetch locally the files from S3
        tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
        files_fetched = fetch_files(s3_uri,
                                    tmp_dir,
                                    regexp='suggested_patterns_summary.json',
                                    force_fetch=force_fetch)
        content = json.loads(open(files_fetched[0][0]).read())
        return cls(content)

    def get(self):
        return self.content


class SuggestedPatternsQuery(object):

    def __init__(self, stream):
        self.stream = stream

    @classmethod
    def from_s3_uri(cls, crawl_id, s3_uri,
                    tmp_dir_prefix='/tmp', force_fetch=False):
        # Fetch locally the files from S3
        tmp_dir = os.path.join(tmp_dir_prefix,
                               'crawl_%d' % crawl_id,
                               'clusters_mixed.tsv')
        fetch_file(os.path.join(s3_uri, 'clusters_mixed.tsv'),
                   tmp_dir, force_fetch=force_fetch)
        return cls(split_file(open(tmp_dir)))

    def get(self):
        for query, query_verbose, hash_id, nb_urls in self.stream:
            yield {
                "query": query,
                "query_verbose": json.loads(query_verbose),
                "query_hash_id": int(hash_id),
                "nb_urls": int(nb_urls)
            }
