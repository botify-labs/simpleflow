# -*- coding: utf-8 -*-
import os
import gzip
import json
import copy

import csv

from pandas import HDFStore, Index

from cdf.exceptions import MissingResource
from cdf.utils.loading import build_dataframe_from_csv
from cdf.streams.caster import Caster
from cdf.streams.utils import split_file
from cdf.utils.s3 import fetch_file, fetch_files, push_file
from cdf.utils.path import makedirs
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.suggestions.constants import CROSS_PROPERTIES_COLUMNS
from cdf.collections.suggestions.aggregator import MetricsAggregator, MetricsConsolidator
from cdf.collections.urls.generators.suggestions import MetadataClusterMixin
from cdf.collections.urls.constants import SUGGEST_CLUSTERS
from cdf.collections.suggestions.query import MetricsQuery, SuggestQuery
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
    source_uri = os.path.join(s3_uri, children_filename)
    destination_path = os.path.join(tmp_dir, children_filename)
    _f, fetched = fetch_file(source_uri,
                             destination_path,
                             force_fetch=force_fetch)

    #build child relationship dataframe
    child_frame = build_dataframe_from_csv(open(_f, "rb"), ["parent", "child"])
    if len(child_frame) > 0:
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

def make_suggest_file_from_query(crawl_id, s3_uri, es_location, es_index, es_doc_type, revision_number, tmp_dir_prefix, identifier, query, urls_fields, urls_filters, urls_sort=None):
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

        if not urls_sort:
            urls_sort = ['id', ]
        urls_query["sort"] = urls_sort

        urls = Query(es_location, es_index, es_doc_type, crawl_id, revision_number, copy.deepcopy(urls_query), start=0, limit=limit)

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
            result["urls_query"] = urls_query
        results.append(result)

    # Write suggestion file
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    summary_file = os.path.join(tmp_dir, 'flat', 'metrics', 'suggest', '{}.json'.format(identifier))
    makedirs(os.path.join(os.path.dirname(summary_file)), exist_ok=True)
    f = open(os.path.join(summary_file), 'w')
    f.write(json.dumps(results, indent=4))
    f.close()
    push_file(
        os.path.join(s3_uri, 'flat', 'metrics', 'suggest', '{}.json'.format(identifier)),
        summary_file
    )
    return len(results)


def make_counter_file_from_query(crawl_id, s3_uri, revision_number, tmp_dir_prefix, identifier, query):
    q = MetricsQuery.from_s3_uri(crawl_id, s3_uri)
    results = q.query(query)
     # Write suggestion file
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    summary_file = os.path.join(tmp_dir, 'flat', 'metrics', '{}.json'.format(identifier))
    makedirs(os.path.join(os.path.dirname(summary_file)), exist_ok=True)
    f = open(os.path.join(summary_file), 'w')
    f.write(json.dumps(results, indent=4))
    f.close()
    push_file(
        os.path.join(s3_uri, 'flat', 'metrics', '{}.json'.format(identifier)),
        summary_file
    )


def make_suggest_summary_file(crawl_id, s3_uri, es_location, es_index, es_doc_type, revision_number, tmp_dir_prefix='/tmp', force_fetch=False):
    counter_kwargs = {
        'crawl_id': crawl_id,
        's3_uri': s3_uri,
        'revision_number': revision_number,
        'tmp_dir_prefix': tmp_dir_prefix,
    }

    suggest_kwargs = counter_kwargs.copy()
    suggest_kwargs.update({
        'es_location': es_location,
        'es_index': es_index,
        'es_doc_type': es_doc_type,
    })

    # Full picture
    query = {}
    make_counter_file_from_query(identifier='full_picture', query=query, **counter_kwargs)

    # Counters by http_code
    query = {
        "group_by": ["http_code"]
    }
    make_counter_file_from_query(identifier='http_code', query=query, **counter_kwargs)

    # Counters by depth
    query = {
        "group_by": ["depth"]
    }
    make_counter_file_from_query(identifier='depth', query=query, **counter_kwargs)

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
        make_suggest_file_from_query(identifier='http_code/{}'.format(str(http_code)[0] + 'xx'), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **suggest_kwargs)

    # Incoming redirections
    query = {
        "fields": ["redirects_from_nb"],
        "target_field": "redirects_from_nb",
    }
    urls_fields = ["redirects_from_nb", "redirects_from"]
    urls_filters = [{
        "field": "redirects_from_nb",
        "value": 0,
        "predicate": "gt"
    }]
    urls_sort = [{"redirects_from_nb": "desc"}]
    make_suggest_file_from_query(identifier='http_code/incoming_redirects', query=query, urls_filters=urls_filters, urls_fields=urls_fields, urls_sort=urls_sort, **suggest_kwargs)

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
            make_suggest_file_from_query(identifier='metadata/{}/{}'.format(metadata_type, metadata_status), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **suggest_kwargs)

    # Speed
    for delay in ("delay_gte_2s", "delay_lt_500ms"):
        urls_fields = ["delay2"]
        urls_filters = get_filters_from_agg_delay_field(delay)
        query = {
            "fields": [delay],
            "target_field": delay
        }
        make_suggest_file_from_query(identifier='delay/{}'.format(delay[6:]), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **suggest_kwargs)

    # Canonicals
    for field in ('filled', 'not_filled', 'equal', 'not_equal', 'incoming'):
        full_field = "canonical_nb.{}".format(field)
        query = {
            "fields": [full_field],
            "target_field": full_field
        }
        if field == "incoming":
            urls_fields = ["canonical_from"]
        else:
            urls_fields = ["canonical_to"]
        urls_filters = get_filters_from_agg_canonical_field(field)
        make_suggest_file_from_query(identifier='canonical/{}'.format(field), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **suggest_kwargs)

    # Deeper depths
    for depth in (3, 5, 7, 10):
        query = {
            "fields": ["pages_nb"],
            "target_field": "pages_nb",
            "filters": {
                "field": "depth",
                "value": depth,
                "predicate": "gte"
            }
        }
        urls_fields = ["depth"]
        urls_filters = [{
            "field": "depth",
            "value": depth,
            "predicate": "gte"
        }]
        make_suggest_file_from_query(identifier='distribution/depth_gte_{}'.format(depth), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **suggest_kwargs)

    # internal outlinks
    for field in ('total', 'follow', 'follow_unique', 'nofollow'):
        full_field = "outlinks_internal_nb.{}".format(field)
        query = {
            "target_field": full_field
        }
        urls_fields = [full_field]
        urls_filters = [
            {"field": full_field, "value": 0, "predicate": "gt"}
        ]
        make_suggest_file_from_query(identifier='outlinks_internal/{}'.format(field), query=query, urls_filters=urls_filters, urls_fields=urls_fields, **suggest_kwargs)

    # broken outlinks
    for field in ('any', '3xx', '4xx', '5xx'):
        full_field = "error_links.{}".format(field)
        query = {
            "target_field": full_field
        }
        urls_fields = [full_field]
        urls_filters = [
            {"field": "error_links.{}.nb".format(field), "value": 0, "predicate": "gt"}
        ]
        urls_sort = [{"error_links.{}.nb".format(field): "desc"}]
        make_suggest_file_from_query(identifier='outlinks_internal/errors_links_{}'.format(field), query=query, urls_filters=urls_filters, urls_fields=urls_fields, urls_sort=urls_sort, **suggest_kwargs)
