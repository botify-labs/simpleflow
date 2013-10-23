# -*- coding: utf-8 -*-
import os
import gzip
import json

from pandas import HDFStore

from cdf.streams.caster import Caster
from cdf.streams.utils import split_file
from cdf.utils.s3 import fetch_file, fetch_files, push_file
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.suggestions.constants import CROSS_PROPERTIES_COLUMNS
from cdf.collections.suggestions.aggregator import MetricsAggregator, MetricsConsolidator
from cdf.collections.urls.generators.suggestions import MetadataClusterMixin
from cdf.collections.urls.constants import SUGGEST_CLUSTERS
from cdf.collections.suggestions.query import SuggestQuery
from cdf.collections.urls.query import Query


def compute_aggregators_from_part_id(crawl_id, s3_uri, part_id, tmp_dir_prefix='/tmp', force_fetch=False):
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(os.path.join(tmp_dir, 'suggest')):
        os.makedirs(os.path.join(tmp_dir, 'suggest'))

    files = ('ids', 'infos',
             '_out_links_counters', '_out_canonical_counters', '_out_redirect_counters',
             '_in_links_counters', '_in_canonical_counters', '_in_redirect_counters',
             '_suggested_clusters', 'contentsduplicate')

    streams = {}
    files_fetched = fetch_files(s3_uri,
                                tmp_dir,
                                regexp=['url(%s).txt.%d.gz' % ('|'.join(files), part_id)],
                                force_fetch=force_fetch)

    for path_local, fetched in files_fetched:
        stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
        cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
        streams["stream_%s" % stream_identifier] = cast(split_file(gzip.open(path_local)))

    # Part seems empty
    if not 'stream_outlinks_counters' in streams:
        return

    aggregator = MetricsAggregator(**streams)
    content = json.dumps(aggregator.get())
    f = open(os.path.join(tmp_dir, 'suggest/counters.{}.json'.format(part_id)), 'w')
    f.write(content)
    f.close()
    push_file(
        os.path.join(s3_uri, 'suggest/counters.{}.json'.format(part_id)),
        os.path.join(tmp_dir, 'suggest/counters.{}.json'.format(part_id))
    )


def consolidate_aggregators(crawl_id, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    """
    Fetch all part_id's aggregators and merge them
    """
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    # Fetch hdf5 file that already contains the full list of requests
    h5_file = os.path.join(tmp_dir, 'suggest.h5')
    if os.path.exists(h5_file):
        os.remove(h5_file)

    # new
    u = MetadataClusterMixin()
    for cluster_type, cluster_name in SUGGEST_CLUSTERS:
        filename = 'clusters_{}_{}.tsv'.format(cluster_type, cluster_name)
        _f, fetched = fetch_file(os.path.join(s3_uri, filename), os.path.join(tmp_dir, filename), force_fetch=force_fetch)
        cluster_values = [k.split('\t', 1)[0] for k in open(_f)]
        if cluster_type == "metadata":
            u.add_metadata_cluster(cluster_name, cluster_values)
        else:
            u.add_pattern_cluster(cluster_name, cluster_values)

    store = HDFStore(h5_file, complevel=9, complib='blosc')
    # Make K/V Store dataframe (hash to request)
    store['requests'] = u.make_clusters_series()

    files_fetched = fetch_files(s3_uri,
                                tmp_dir,
                                regexp=['suggest/counters.([0-9]+).json'],
                                force_fetch=force_fetch)

    counters = [json.load(open(path_local)) for path_local, fetched in files_fetched]
    c = MetricsConsolidator(counters)
    df_counter = c.get_dataframe()
    store["full_crawl"] = df_counter[df_counter['query'] == '0'].groupby(CROSS_PROPERTIES_COLUMNS).agg('sum').reset_index()
    store["suggest"] = df_counter[df_counter['query'] != '0'].groupby(CROSS_PROPERTIES_COLUMNS).agg('sum').reset_index()

    store.close()
    push_file(os.path.join(s3_uri, 'suggest.h5'), h5_file)


def make_suggest_summary_file(crawl_id, s3_uri, es_location, es_index, es_doc_type, rev_num, tmp_dir_prefix='/tmp', force_fetch=False):
    query_type = []
    queries = []
    urls_fields = []
    urls_filters = []
    for http_code in (300, 400, 500):
        query_type.append(['http_code', str(http_code)[0] + 'xx'])
        queries.append(
            {
                "fields": ["pages_nb"],
                "target_field": "pages_nb",
                "filters": {
                    'and': [
                        {"field": "http_code", "value": http_code, "predicate": "gte"},
                        {"field": "http_code", "value": http_code + 99, "predicate": "lt"},
                    ]
                }
            }
        )
        if http_code == 300:
            urls_fields.append(["redirects_to"])
        else:
            urls_fields.append(["http_code"])
        urls_filters.append(
            [
                {"field": "http_code", "value": http_code, "predicate": "gte"},
                {"field": "http_code", "value": http_code + 99, "predicate": "lt"},
            ]
        )
    for metadata_type in ('title', 'description', 'h1'):
        for metadata_status in ('duplicate', 'not_filled'):
            query_type.append(['metadata', metadata_type, metadata_status])
            queries.append(
                {
                    "fields": ["pages_nb", "metadata_nb.{}.{}".format(metadata_type, metadata_status)],
                    "target_field": "metadata_nb.{}.{}".format(metadata_type, metadata_status)
                }
            )
            if metadata_status == "duplicate":
                urls_fields.append(["metadata.{}".format(metadata_type), "metadata_duplicate.{}".format(metadata_type)])
                urls_filters.append([
                    {"field": "metadata_duplicate_nb.{}".format(metadata_type), "value": 1, "predicate": "gt"}
                ])
            else:
                urls_fields.append([])
                urls_filters.append([
                    {"field": "metadata_nb.{}".format(metadata_type), "value": 0}
                ])

    final_summary = []
    q = SuggestQuery.from_s3_uri(crawl_id, s3_uri)
    for i, query in enumerate(queries):
        results = q.query(query)
        for result in results:
            hash_id_filters = [{'field': 'patterns', 'value': hash_id} for hash_id in result['query_hash_id']]
            urls_query = {
                "fields": ["url"] + urls_fields[i],
                "filters": {'and': hash_id_filters + urls_filters[i]}
            }
            urls = Query(es_location, es_index, es_doc_type, crawl_id, rev_num, urls_query, start=0, limit=10, sort=('id',))
            final_summary.append(
                {
                    "type": query_type[i],
                    "results": result,
                    "score": reduce(dict.get, query["target_field"].split("."), result["counters"]),
                    "urls": list(urls.results)
                }
            )

    final_summary = sorted(final_summary, key=lambda i: i['score'], reverse=True)
    final_summary_flatten = json.dumps(final_summary, indent=4)
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(os.path.join(tmp_dir)):
        os.makedirs(os.path.join(tmp_dir))

    summary_file = os.path.join(tmp_dir, 'suggested_patterns_summary.json')
    f = open(os.path.join(summary_file), 'w')
    f.write(final_summary_flatten)
    f.close()

    push_file(
        os.path.join(s3_uri, 'suggested_patterns_summary.json'),
        summary_file
    )

    print final_summary_flatten
