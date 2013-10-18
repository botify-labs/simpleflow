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


def compute_aggregators_from_part_id(crawl_id, s3_uri, part_id, tmp_dir_prefix='/tmp', force_fetch=False):
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(os.path.join(tmp_dir, 'suggest')):
        os.makedirs(os.path.join(tmp_dir, 'suggest'))

    streams = {}
    files_fetched = fetch_files(s3_uri,
                                tmp_dir,
                                regexp=['url(ids|infos|links|inlinks|_suggested_clusters|contentsduplicate).txt.%d.gz' % part_id],
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
    fetch_file(
        os.path.join(s3_uri, 'suggest.h5'),
        h5_file,
        force_fetch=force_fetch
    )

    store = HDFStore(h5_file, complevel=9, complib='blosc')

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
