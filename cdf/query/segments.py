import os
from StringIO import StringIO
from collections import defaultdict

from cdf.compat import json

from cdf.query.query import QueryBuilder
from cdf.metadata.url.es_backend_utils import ElasticSearchBackend
from cdf.core.metadata.dataformat import generate_data_format
from cdf.utils import s3

FILTER_LEAF = 'FILTER_LEAF'
FILTER_ROOT = 'FILTER_ROOT'


def get_segments_from_query(query, es_location, es_index, es_doc_type,
                            crawl_id, features_options, s3_uri, apply_filter=None):
    """
    Return a list of segments from a given query
    """
    data_format = generate_data_format(features_options)
    data_backend = ElasticSearchBackend(data_format)
    query_builder = QueryBuilder(es_location, es_index, es_doc_type,
                                 crawl_id, features_options, data_backend)

    # Fetch queyr aggs results
    query["aggs"] = [{
        "group_by": ["patterns"],
        "metric": "count",
    }]
    query_obj = query_builder.get_aggs_query(query)
    results = query_obj.aggs[0]["groups"]

    # It's currently not possible to sort by most frequent pattern
    # on elasticsearch (see http://www.elastic.co/guide/en/elasticsearch/reference/1.x/search-aggregations-metrics-valuecount-aggregation.html
    results = sorted(results, key=lambda i: i["metrics"][0], reverse=True)

    f_names = s3.get_content_to_streamio_file(os.path.join(s3_uri, 'clusters_mixed.tsv'))
    f_relationships = s3.get_content_to_streamio_file(os.path.join(s3_uri, 'cluster_mixed_children.tsv'))
    segments = load_segments_from_files(f_names, f_relationships)

    return get_segments_from_args(results, segments, apply_filter=apply_filter)



def get_segments_from_args(agg_results, segments, apply_filter=None):
    """
    :param agg_results = a list of dicts like : {'groups': [{'key': [10873], 'metrics': [1]}]}
    :param segments : A list of segments dicts {"human": ..., "query": ..., "total_urls": 10})
    :param
    Return a list of most frequent segments from a BQL query
    """
    segments_idx = {s["hash"]:s for s in segments}
    results = []
    for agg in agg_results:
        segment = segments_idx[agg["key"][0]]

        # Filters
        if apply_filter == FILTER_LEAF:
            if len(segment["children"]) > 0:
                continue
        if apply_filter == FILTER_ROOT:
            if segment["parent"] is not None:
                continue

        results.append({
            "segment": {
                "query": segment["query"],
                "human": segment["human"],
                "total_urls": segment["total_urls"]
            },
            "nb_urls": agg["metrics"][0]
        })
    return results


def load_segments_from_files(f_names, f_relationships):
    """
    Load segments from file and return it as a list of dict objects

    TSV file format is :
    * human readble query
    * Botify Query
    * Query hash
    * Number of url matching this query

    Key is segment hash
    Value is a dict with the following format :
    {
        "human": "host='www.site.com' and path='products/'",
        "query": {
            "and": [
                {"field": "host", "value": "www.site.com"},
                {"field": "path", "value": "products/", "predicate": "startswith"}
            ]
        },
        "hash": 11229837,
        "children": [1227267, 192762],
        "parent": 1836753,
        "total_urls": 10
    }
    """

    # Prepare relationships dicts
    rel_children = defaultdict(list)
    rel_parent = {}
    for line in f_relationships:
        parent, child = line[:-1].split('\t')
        parent = int(parent)
        child = int(child)
        rel_children[parent].append(child)
        rel_parent[child] = parent

    segments = []
    for line in f_names:
        human, query, _hash, total_urls = line[:-1].split('\t')
        _hash = int(_hash)
        segments.append({
            "human": human,
            "query": json.loads(query),
            "hash": int(_hash),
            "total_urls": int(total_urls),
            "parent": rel_parent.get(_hash, None),
            "children": rel_children[_hash]
        })
    return segments

