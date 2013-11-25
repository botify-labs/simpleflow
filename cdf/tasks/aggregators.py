# -*- coding: utf-8 -*-
import os
import gzip
import json

import csv

from pandas import HDFStore, DataFrame, Index

from cdf.exceptions import MissingResource
from cdf.streams.caster import Caster
from cdf.streams.utils import split_file
from cdf.utils.s3 import fetch_file, fetch_files, push_file
from cdf.utils.path import makedirs
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.suggestions.constants import CROSS_PROPERTIES_COLUMNS
from cdf.collections.suggestions.aggregator import MetricsAggregator, MetricsConsolidator
from cdf.collections.urls.generators.suggestions import MetadataClusterMixin
from cdf.collections.urls.constants import SUGGEST_CLUSTERS
from cdf.collections.suggestions.query import SuggestQuery
from cdf.collections.urls.query import Query
from cdf.collections.urls.query_helpers import (
    get_filters_from_http_code_range,
    get_filters_from_agg_delay_field,
    get_filters_from_agg_canonical_field
)


def compute_aggregators_from_part_id(crawl_id, s3_uri, part_id, tmp_dir_prefix='/tmp', force_fetch=False):
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    suggest_dir_path = os.path.join(tmp_dir, 'suggest')
    makedirs(suggest_dir_path, exist_ok=True)

    files = ('ids', 'infos',
             '_out_links_counters', '_out_canonical_counters', '_out_redirect_counters',
             '_in_links_counters', '_in_canonical_counters', '_in_redirect_counters',
             '_suggested_clusters',
             'contentsduplicate',
             'badlinks_counters')
    mandatory_files = ('ids', 'infos')

    streams = {}
    fetched_files = []
    missing_files = []
    for file_type in files:
        filename = 'url%s.txt.%d.gz' % (file_type, part_id)
        crt_fetched_files = fetch_files(s3_uri,
                                        tmp_dir,
                                        regexp=[filename],
                                        force_fetch=force_fetch)
        if len(crt_fetched_files) == 0:
            if file_type in mandatory_files:
                raise MissingResource("Could not fetch file : {}".format(filename))
            missing_files.append(os.path.join(s3_uri, filename))
        else:
            fetched_files.extend(crt_fetched_files)

    # Create an empty stream for all missing files (not mandatory)
    if missing_files:
        for filename in missing_files:
            stream_identifier = STREAMS_FILES[os.path.basename(filename).split('.')[0]]
            streams["stream_{}".format(stream_identifier)] = iter([])

    for path_local, fetched in fetched_files:
        stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
        cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
        streams["stream_%s" % stream_identifier] = cast(split_file(gzip.open(path_local)))

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
    makedirs(tmp_dir, exist_ok=True)

    # Fetch hdf5 file that already contains the full list of requests
    h5_file = os.path.join(tmp_dir, 'suggest.h5')
    if os.path.exists(h5_file):
        os.remove(h5_file)

    # new
    u = MetadataClusterMixin()
    for cluster_type in SUGGEST_CLUSTERS:
        filename = 'clusters_{}.tsv'.format(cluster_type)

        source_uri = os.path.join(s3_uri, filename)
        destination_path = os.path.join(tmp_dir, filename)

        file_, fetched = fetch_file(source_uri,
                                    destination_path,
                                    force_fetch=force_fetch)

        cluster_values = []
        csv_reader = csv.reader(open(file_),
                                delimiter="\t",
                                quotechar=None,
                                quoting=csv.QUOTE_NONE)
        for row in csv_reader:
            pattern, verbose_pattern, hash, _ = row
            cluster_values.append((pattern, verbose_pattern, hash))
        u.add_pattern_cluster(cluster_type, cluster_values)

    store = HDFStore(h5_file, complevel=9, complib='blosc')
    # Make K/V Store dataframe (hash to request)
    store['requests'] = u.make_clusters_dataframe()

    #fetch child relationship tsv
    children_filename = "cluster_mixed_children.tsv"
    source_uri = os.path.join(s3_uri, filename)
    destination_path = os.path.join(tmp_dir, children_filename)
    _f, fetched = fetch_file(source_uri,
                             destination_path,
                             force_fetch=force_fetch)

    #build child relationship dataframe
    csv_reader = csv.reader(open(_f, "rb"),
                            delimiter="\t",
                            quotechar=None,
                            quoting=csv.QUOTE_NONE)
    row_list = [row for row in csv_reader]
    if len(row_list) > 0:
        child_frame = DataFrame([r[2:] for r in row_list], columns=["parent", "child"])
        #store dataframe in hdfstore.
        #we do not store empty dataframe in hdfstore since recovering it
        #afterwards raises an exception :
        #ValueError: Shape of passed values is (2, 0), indices imply (2, 1)
        store['children'] = child_frame

    files_fetched = fetch_files(s3_uri,
                                tmp_dir,
                                regexp=['suggest/counters.([0-9]+).json'],
                                force_fetch=force_fetch)

    counters = [json.load(open(path_local)) for path_local, fetched in files_fetched]
    c = MetricsConsolidator(counters)
    df_counter = c.get_dataframe()
    store["full_crawl"] = df_counter[df_counter['query'] == '0'].groupby(CROSS_PROPERTIES_COLUMNS).agg('sum').reset_index()
    suggest_frame = df_counter[df_counter['query'] != '0'].groupby(CROSS_PROPERTIES_COLUMNS).agg('sum').reset_index()
    if len(suggest_frame) == 0:
        #for an unknown reason, pandas raises a :
        #ValueError: Shape of passed values is (75, 0), indices imply (75, 1)
        #
        #when retrieving a dataframe with an empty int64 index.
        #To avoid this, we change the type of the index.
        #It does not hurt, since the dataframe is empty.
        suggest_frame.index = Index([], dtype=int)
    store["suggest"] = suggest_frame

    store.close()
    push_file(os.path.join(s3_uri, 'suggest.h5'), h5_file)


def make_suggest_file_from_query(crawl_id, s3_uri, es_location, es_index, es_doc_type, revision_number, tmp_dir_prefix, identifier, query, urls_fields, urls_filters):
    q = SuggestQuery.from_s3_uri(crawl_id, s3_uri)
    query["display_children"] = False
    _results = q.query(query)
    results = []
    for k, result in enumerate(_results):
        hash_id_filters = [{'field': 'patterns', 'value': result['query_hash_id']}]
        urls_query = {
            "fields": ["url"] + urls_fields,
            "filters": {'and': hash_id_filters + urls_filters}
        }

        result["score"] = reduce(dict.get, query["target_field"].split("."), result["counters"])
        if result["score"] == 0:
            continue

        if identifier.startswith("http_code") or identifier.startswith("metadata:not_filled"):
            limit = 3
        else:
            limit = 10

        urls = Query(es_location, es_index, es_doc_type, crawl_id, revision_number, urls_query, start=0, limit=limit, sort=('id',))

        urls_results = list(urls.results)
        result["urls"] = []
        # Filter on metadata duplicate : get only the 3 first different duplicates urls
        if identifier.startswith("metadata:duplicate"):
            duplicates_found = set()
            for url_result in urls_results:
                metadata_value = url_result["metadata"][identifier.rsplit(':', 1)[1]][0]
                if metadata_value not in duplicates_found:
                    result["urls"].append(url_result)
                    duplicates_found.add(metadata_value)
                    if len(duplicates_found) == 3:
                        break
        else:
            result["urls"] = urls_results
        results.append(result)

    # Write suggestion file
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    summary_file = os.path.join(tmp_dir, 'suggest', '{}.json'.format(identifier))
    makedirs(os.path.join(os.path.dirname(summary_file)), exist_ok=True)
    f = open(os.path.join(summary_file), 'w')
    f.write(json.dumps(results, indent=4))
    f.close()
    push_file(
        os.path.join(s3_uri, 'suggest', '{}.json'.format(identifier)),
        summary_file
    )
    return len(results)



def make_suggest_summary_file(crawl_id, s3_uri, es_location, es_index, es_doc_type, revision_number, tmp_dir_prefix='/tmp', force_fetch=False):
    summary_kwargs = {
        'crawl_id': crawl_id,
        's3_uri': s3_uri,
        'es_location': es_location,
        'es_index': es_index,
        'es_doc_type': es_doc_type,
        'revision_number': revision_number,
        'tmp_dir_prefix': tmp_dir_prefix,
    }

    # Http codes by range
    for http_code in (200, 300, 400, 500):
        query = {
            "fields": ["pages_nb"],
            "target_field": "pages_nb",
            "filters": {
                'and': [
                    {"field": "http_code", "value": http_code, "predicate": "gte"},
                    {"field": "http_code", "value": http_code + 99, "predicate": "lt"},
                ]
            }
        }
        if http_code == 300:
            urls_fields = ["redirects_to"]
        else:
            urls_fields = ["http_code"]
        urls_filters = get_filters_from_http_code_range(http_code)
        make_suggest_file_from_query(identifier='http_code:{}'.format(str(http_code)[0] + 'xx'), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **summary_kwargs)

    # Metadata types
    for metadata_type in ('title', 'description', 'h1'):
        for metadata_status in ('duplicate', 'not_filled'):
            query = {
                "fields": ["pages_nb", "metadata_nb.{}".format(metadata_type), "metadata_duplicate_nb.{}".format(metadata_type)],
                "target_field": "metadata_nb.{}.{}".format(metadata_type, metadata_status)
            }
            if metadata_status == "duplicate":
                urls_fields = ["metadata.{}".format(metadata_type), "metadata_duplicate.{}".format(metadata_type), "metadata_duplicate_nb.{}".format(metadata_type)]
                urls_filters = [
                    {"field": "metadata_duplicate_nb.{}".format(metadata_type), "value": 1, "predicate": "gt"}
                ]
            else:
                urls_fields = []
                urls_filters = [
                    {"field": "metadata_nb.{}".format(metadata_type), "value": 0}
                ]
            make_suggest_file_from_query(identifier='metadata:{}:{}'.format(metadata_type, metadata_status), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **summary_kwargs)

    # Speed
    for delay in ("delay_gte_2s", "delay_lt_500ms"):
        urls_fields = ["delay2"]
        urls_filters = get_filters_from_agg_delay_field(delay)
        query = {
            "fields": [delay],
            "target_field": delay
        }
        make_suggest_file_from_query(identifier='delay:{}'.format(delay[6:]), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **summary_kwargs)

    # Canonicals
    for field in ('filled', 'not_filled', 'equal', 'not_equal', 'incoming'):
        full_field = "canonical_nb.{}".format(field)
        query = {
            "fields": [full_field],
            "target_field": full_field
        }
        urls_fields = ["canonical_to", "canonical_from"]
        urls_filters = get_filters_from_agg_canonical_field(field)
        make_suggest_file_from_query(identifier='canonical:{}'.format(field), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **summary_kwargs)

    # Deeper depths
    for depth in (5, 7, 10):
        query = {
            "filters": {
                "field": "depth",
                "value": depth,
                "predicate": "gte"
            }
        }
        urls_fields = []
        urls_filters = [{
            "field": "depth",
            "value": depth,
            "predicate": "gte"
        }]
        make_suggest_file_from_query(identifier='distribution:depth_gte_{}'.format(depth), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **summary_kwargs)
