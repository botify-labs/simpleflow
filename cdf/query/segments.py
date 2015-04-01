import os
from StringIO import StringIO

from cdf.compat import json

from cdf.query.query import QueryBuilder
from cdf.metadata.url.es_backend_utils import ElasticSearchBackend
from cdf.core.metadata.dataformat import generate_data_format
from cdf.utils import s3


def get_segments_from_query(query, es_location, es_index, es_doc_type,
                            crawl_id, features_options, s3_uri):
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

    # Load segments idx
    segments_idx = load_segments_idx_from_s3(os.path.join(s3_uri, 'clusters_mixed.tsv'))

    return get_segments_from_args(results, segments_idx)


def get_segments_from_args(agg_results, segments_idx):
    """
    :param agg_results = a list of dicts like : {'groups': [{'key': [10873], 'metrics': [1]}]}
    :parem segments_idx : A dict of segments (key : hash, value : {"human": ..., "query": ..., "total_urls": 10})
    Return a list of most frequent segments from a BQL query
    """
    results = []
    for agg in agg_results:
        results.append({
            "segment": segments_idx[agg["key"][0]],
            "nb_urls": agg["metrics"][0]
        })
    return results


def load_segments_idx_from_s3(s3_uri):
    f = StringIO()
    key = s3.get_key_from_s3_uri(s3_uri)
    key.get_contents_to_file(f)
    f.seek(0)
    segments = load_segments_idx_from_file(f)
    f.close()
    return segments


def load_segments_idx_from_file(f):
    """
    Load segments from file and return it as a dict

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
        "total_urls": 10
    }
    """
    segments_idx = {}
    for line in f:
        human, query, _hash, total_urls = line[:-1].split('\t')
        segments_idx[int(_hash)] = {
            "human": human,
            "query": json.loads(query),
            "total_urls": int(total_urls)
        }
    return segments_idx

