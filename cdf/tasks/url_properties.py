import os
import gzip
import lz4
import json
import itertools

from pandas import HDFStore
from elasticsearch import Elasticsearch

from cdf.log import logger
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.streams.caster import Caster
from cdf.streams.utils import split_file, split
from cdf.collections.urls.generators.tagging import UrlTaggingGenerator
from cdf.collections.tagging_stats.aggregator import (MetricsAggregator, MetricsConsolidator,
                                                      MetadataAggregator)
from cdf.utils.s3 import fetch_file, fetch_files, push_content, push_file
from cdf.utils.es import bulk
from cdf.utils.remote_files import nb_parts_from_crawl_location


def compute_properties_from_s3(crawl_id, part_id, rev_num, s3_uri, settings, es_location, es_index, es_doc_type, tmp_dir_prefix='/tmp', force_fetch=False, erase_old_tagging=False):
    """
    Match all urls from a crawl's `part_id` to properties defined by rules in a `settings` dictionnary and save it to a S3 bucket.

    Crawl dataset for this part_id is found by fetching all files finishing by .txt.[part_id] in the `s3_uri` called.

    :param part_id : the part_id from the crawl
    :param s3_uri : the location where the file will be pushed. filename will be url_properties.txt.[part_id]
    :param settings : a settings dictionnary
    :param es_location : elastic search location (ex: http://localhost:9200)
    :param es_index : index name where to push the documents.
    :param es_doc_type : doc_type name where to push the documents.
    :param tmp_dir : the temporary directory where the S3 files will be put to compute the task
    :param force_fetch : fetch the S3 files even if they are already in the temp directory
    :param erase_old_tagging : will remove nested tagging for old revisions
    """
    host, port = es_location[7:].split(':')
    es = Elasticsearch([{'host': host, 'port': int(port)}])

    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    files_fetched = fetch_files(s3_uri,
                                tmp_dir,
                                regexp=['url(ids|infos).txt.%d.gz' % part_id],
                                force_fetch=force_fetch)

    streams = dict()
    for path_local, fetched in files_fetched:
        stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
        cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
        streams[stream_identifier] = cast(split_file(gzip.open(path_local)))

    g = UrlTaggingGenerator(streams['patterns'], streams['infos'], settings)

    docs = []
    raw_lines = []
    for i, document in enumerate(g):
        if erase_old_tagging:
            es_script = "ctx._source.tagging = [tagging]"
        else:
            es_script = """if (ctx._source[\"tagging\"] == null) { ctx._source.tagging = [tagging] } else {
                       ctx._source.tagging += tagging }"""
        doc = {
            "_id": "{}.{}".format(crawl_id, document[0]),
            "script": es_script,
            "params": {
                "tagging": {
                    "resource_type": document[1]['resource_type'],
                    "rev_id": rev_num
                }
            }
        }
        docs.append(doc)
        raw_lines.append('\t'.join((str(document[0]), document[1]['resource_type'])))
        if i % 10000 == 9999:
            bulk(es, docs, doc_type=es_doc_type, index=es_index, bulk_type="update")
            docs = []
            logger.info('%d items updated to crawl_%d ES for %s (part %d)' % (i, crawl_id, es_index, part_id))
    # Push the missing documents
    if docs:
        bulk(es, docs, doc_type=es_doc_type, index=es_index, bulk_type="update")

    content = '\n'.join(raw_lines)

    encoded_content = lz4.dumps(content)
    push_content(os.path.join(s3_uri, 'url_properties.rev%d.txt.%d.lz4' % (rev_num, part_id)), encoded_content)


def compute_properties_stats_counter_from_s3(crawl_id, part_id, rev_num, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    streams = {}

    properties_file = fetch_file(
        os.path.join(s3_uri, 'url_properties.rev%d.txt.%d.lz4' % (rev_num, part_id)),
        os.path.join(tmp_dir, 'url_properties.rev%d.txt.%d.lz4' % (rev_num, part_id)),
        force_fetch=force_fetch)
    path_local, fetch = properties_file
    file_content = lz4.loads(open(path_local).read())
    if not file_content:
        return
    cast = Caster(STREAMS_HEADERS["PROPERTIES"]).cast
    streams["stream_properties"] = cast(split(file_content.split('\n')))

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
    push_content(os.path.join(s3_uri, 'properties_stats_partial_rev%d/stats.json.%d' % (rev_num, part_id)), content)


def _get_df_properties_stats_counter_from_s3(crawl_id, rev_num, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    """
    Consolidate all properties stats counter from different parts of a crawl and return a dataframe
    """
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    files_fetched = fetch_files(s3_uri,
                                tmp_dir,
                                regexp=['properties_stats_partial_rev%d/stats.json' % rev_num],
                                force_fetch=force_fetch)

    counters = [json.load(open(path_local)) for path_local, fetched in files_fetched]

    # Append metadata
    counters.append(_get_df_properties_stats_meta_from_s3(crawl_id, rev_num, s3_uri, tmp_dir_prefix, force_fetch))

    c = MetricsConsolidator(counters)
    return c.get_dataframe()


def _get_df_properties_stats_meta_from_s3(crawl_id, rev_num, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    """
    Fetch contents streams to generate a dataframe containing the unicity of metadata (h1, title, meta description, h2) by cross-property
    """

    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    streams_types = {'patterns': [],
                     'properties': [],
                     'contents': [],
                     'infos': []
                     }

    for part_id in xrange(0, nb_parts_from_crawl_location(s3_uri)):
        properties_file = fetch_file(
            os.path.join(s3_uri, 'url_properties.rev%d.txt.%d.lz4' % (rev_num, part_id)),
            os.path.join(tmp_dir, 'url_properties.rev%d.txt.%d.lz4' % (rev_num, part_id)),
            force_fetch=force_fetch)
        path_local, fetch = properties_file
        file_content = lz4.loads(open(path_local).read())
        if not file_content:
            # If no content for properties file, that means that no pages were crawled for this part_id, we can skip
            continue

        cast = Caster(STREAMS_HEADERS["PROPERTIES"]).cast
        streams_types["properties"].append(cast(split(file_content.split('\n'))))

        files_fetched = fetch_files(s3_uri,
                                    tmp_dir,
                                    regexp=['url(ids|contents|infos).txt.%d.gz' % part_id],
                                    force_fetch=force_fetch)

        for path_local, fetched in files_fetched:
            stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
            cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
            streams_types[stream_identifier].append(cast(split_file(gzip.open(path_local))))

    a = MetadataAggregator(itertools.chain(*streams_types['patterns']),
                           itertools.chain(*streams_types['properties']),
                           itertools.chain(*streams_types['contents']),
                           itertools.chain(*streams_types['infos']))
    return a.get()


def compute_properties_stats_from_s3(crawl_id, rev_num, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    h5_file = os.path.join(tmp_dir, 'properties_stats_rev%d.h5' % rev_num)
    if os.path.exists(h5_file):
        os.remove(h5_file)

    store = HDFStore(h5_file, complevel=9, complib='blosc')
    store['counter'] = _get_df_properties_stats_counter_from_s3(crawl_id, rev_num, s3_uri, tmp_dir_prefix, force_fetch)
    store.close()

    push_file(os.path.join(s3_uri, 'properties_stats_rev%d.h5' % rev_num), h5_file)
