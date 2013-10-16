# -*- coding: utf-8 -*-
import os
import gzip
import json
import lz4
import itertools

from pandas import HDFStore

from cdf.streams.caster import Caster
from cdf.streams.utils import split_file, split
from cdf.collections.urls.generators.suggestions import UrlSuggestionsGenerator
from cdf.utils.s3 import fetch_file, fetch_files, push_content, push_file
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.suggestions.constants import CROSS_PROPERTIES_COLUMNS
from cdf.collections.urls.constants import SUGGEST_CLUSTERS
from cdf.collections.suggestions.aggregator import (MetricsAggregator, MetricsConsolidator,
                                                    MetadataAggregator)
from cdf.utils.remote_files import nb_parts_from_crawl_location
from cdf.log import logger


def make_url_to_suggested_patterns_file(crawl_id, part_id, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    """
    Match all urls with suggested patterns coming for precomputed clusters

    Crawl dataset for this part_id is found by fetching all files finishing by .txt.[part_id] in the `s3_uri` called.

    :param part_id : the part_id from the crawl
    :param s3_uri : the location where the file will be pushed. filename will be url_properties.txt.[part_id]
    :param tmp_dir : the temporary directory where the S3 files will be put to compute the task
    :param force_fetch : fetch the S3 files even if they are already in the temp directory
    """
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(tmp_dir):
        try:
            os.makedirs(tmp_dir)
        except:
            pass

    files_fetched = fetch_files(s3_uri,
                                tmp_dir,
                                regexp=['url(ids|infos|contents).txt.%d.gz' % part_id],
                                force_fetch=force_fetch)

    streams = dict()
    for path_local, fetched in files_fetched:
        stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
        cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
        streams[stream_identifier] = cast(split_file(gzip.open(path_local)))

    u = UrlSuggestionsGenerator(streams['patterns'], streams['infos'], streams['contents'])

    for cluster_type, cluster_name in SUGGEST_CLUSTERS:
        filename = 'clusters_{}_{}.tsv'.format(cluster_type, cluster_name)
        _f, fetched = fetch_file(os.path.join(s3_uri, filename), os.path.join(tmp_dir, filename), force_fetch=force_fetch)
        cluster_values = [k.split('\t', 1)[0] for k in open(_f)]
        if cluster_type == "metadata":
            u.add_metadata_cluster(cluster_name, cluster_values)
        else:
            u.add_pattern_cluster(cluster_name, cluster_values)

    # Make K/V Store dataframe (hash to request)
    h5_file = os.path.join(tmp_dir, 'suggest.h5')
    if os.path.exists(h5_file):
        os.remove(h5_file)

    store = HDFStore(h5_file, complevel=9, complib='blosc')
    store['requests'] = u.make_clusters_series()
    store.close()
    push_file(os.path.join(s3_uri, 'suggest.h5'), h5_file)

    content = []
    for i, result in enumerate(u):
        # TODO : bench best method to write line
        content.append('\t'.join((str(i) for i in result)))
        if i % 1000 == 999:
            logger.info(content[-1])
    encoded_content = lz4.dumps('\n'.join(content))
    push_content(os.path.join(s3_uri, 'url_suggested_clusters.{}.txt.lz4'.format(part_id)), encoded_content)
    push_content(os.path.join(s3_uri, 'url_suggested_clusters.{}.txt'.format(part_id)), '\n'.join(content))


def compute_suggestions_counter_from_s3(crawl_id, part_id, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    streams = {}

    properties_file = fetch_file(
        os.path.join(s3_uri, 'url_suggested_clusters.{}.txt.lz4'.format(part_id)),
        os.path.join(tmp_dir, 'url_suggested_clusters.{}.txt.lz4'.format(part_id)),
        force_fetch=force_fetch)
    path_local, fetch = properties_file
    file_content = lz4.loads(open(path_local).read())
    if not file_content:
        return
    cast = Caster(STREAMS_HEADERS["SUGGEST"]).cast
    streams["stream_suggest"] = cast(split(file_content.split('\n')))

    files_fetched = fetch_files(s3_uri,
                                tmp_dir,
                                regexp=['url(ids|infos|links|inlinks).txt.%d.gz' % part_id],
                                force_fetch=force_fetch)

    for path_local, fetched in files_fetched:
        stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
        cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
        streams["stream_%s" % stream_identifier] = cast(split_file(gzip.open(path_local)))

    # Not crawled urls may be referenced in urlids and urlinfos files, but some files may be missing like inlinks...
    for optional_stream in ('outlinks', 'inlinks'):
        stream_key = "stream_{}".format(optional_stream)
        if not stream_key in streams:
            streams[stream_key] = iter([])

    aggregator = MetricsAggregator(**streams)
    content = json.dumps(aggregator.get())
    push_content(os.path.join(s3_uri, 'suggestions/counters.{}.json'.format(part_id)), content)


def _get_df_suggest_counter_from_s3(crawl_id, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    """
    Consolidate all properties stats counter from different parts of a crawl and return a dataframe
    """
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    files_fetched = fetch_files(s3_uri,
                                tmp_dir,
                                regexp=['suggestions/counters.([0-9]+).json'],
                                force_fetch=force_fetch)

    counters = [json.load(open(path_local)) for path_local, fetched in files_fetched]

    # Append metadata
    counters.append(_get_df_suggest_meta_from_s3(crawl_id, s3_uri, tmp_dir_prefix, force_fetch))
    c = MetricsConsolidator(counters)
    return c.get_dataframe()


def _get_df_suggest_meta_from_s3(crawl_id, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    """
    Fetch contents streams to generate a dataframe containing the unicity of metadata (h1, title, meta description, h2) by cross-property
    """

    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    streams_types = {'patterns': [],
                     'suggest': [],
                     'contents': [],
                     'infos': []
                     }

    for part_id in xrange(0, nb_parts_from_crawl_location(s3_uri)):
        properties_file = fetch_file(
            os.path.join(s3_uri, 'url_suggested_clusters.{}.txt.lz4'.format(part_id)),
            os.path.join(tmp_dir, 'url_suggested_clusters.{}.txt.lz4'.format(part_id)),
            force_fetch=force_fetch)
        path_local, fetch = properties_file
        file_content = lz4.loads(open(path_local).read())
        if not file_content:
            # If no content for properties file, that means that no pages were crawled for this part_id, we can skip
            continue

        cast = Caster(STREAMS_HEADERS["SUGGEST"]).cast
        streams_types["suggest"].append(cast(split(file_content.split('\n'))))

        files_fetched = fetch_files(s3_uri,
                                    tmp_dir,
                                    regexp=['url(ids|contents|infos).txt.%d.gz' % part_id],
                                    force_fetch=force_fetch)

        for path_local, fetched in files_fetched:
            stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
            cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
            streams_types[stream_identifier].append(cast(split_file(gzip.open(path_local))))

    a = MetadataAggregator(itertools.chain(*streams_types['patterns']),
                           itertools.chain(*streams_types['suggest']),
                           itertools.chain(*streams_types['contents']),
                           itertools.chain(*streams_types['infos']))
    return a.get()


def compute_suggest_aggregators_from_s3(crawl_id, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    # Fetch hdf5 file that already contains the full list of requests
    h5_file = os.path.join(tmp_dir, 'suggest.h5')
    fetch_file(
        os.path.join(s3_uri, 'suggest.h5'),
        h5_file,
        force_fetch=force_fetch
    )

    store = HDFStore(h5_file, complevel=9, complib='blosc')
    df_counter = _get_df_suggest_counter_from_s3(crawl_id, s3_uri, tmp_dir_prefix, force_fetch)
    store["full_crawl"] = df_counter[df_counter['query'] == '0'].groupby(CROSS_PROPERTIES_COLUMNS).agg('sum').reset_index()
    store["suggest"] = df_counter[df_counter['query'] != '0'].groupby(CROSS_PROPERTIES_COLUMNS).agg('sum').reset_index()

    store.close()
    push_file(os.path.join(s3_uri, 'suggest.h5'), h5_file)
