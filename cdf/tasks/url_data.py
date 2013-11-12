import os
import gzip
import lz4

from elasticsearch import Elasticsearch

from cdf.constants import URLS_DATA_MAPPING
from cdf.log import logger
from cdf.utils.s3 import fetch_files
from cdf.utils.es import bulk
from cdf.streams.caster import Caster
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.urls.generators.documents import UrlDocumentGenerator
from cdf.streams.utils import split_file, split


def prepare_crawl_index(crawl_id, es_location, es_index, es_doc_type):
    host, port = es_location[7:].split(':')
    es = Elasticsearch([{'host': host, 'port': int(port)}])
    try:
        es.indices.create(es_index)
    except Exception, e:
        logger.error("{} : {}".format(type(e), str(e)))
    es.indices.put_mapping(es_index, es_doc_type, URLS_DATA_MAPPING)


def push_urls_to_elastic_search(crawl_id, part_id, s3_uri, es_location, es_index, es_doc_type, tmp_dir_prefix='/tmp', force_fetch=False):
    """
    Generate JSON type urls documents from a crawl's `part_id` and push it to elastic search

    Crawl dataset for this part_id is found by fetching all files finishing by .txt.[part_id] in the `s3_uri` called.

    :param part_id : part_id of the crawl
    :param s3_uri : location where raw files are fetched
    :param es_location : elastic search location (ex: http://localhost:9200)
    :param es_index : index name where to push the documents.
    :param tmp_dir : temporary directory where the S3 files are fetched to compute the task
    :param force_fetch : fetch the S3 files even if they are already in the temp directory
    """

    host, port = es_location[7:].split(':')
    es = Elasticsearch([{'host': host, 'port': int(port)}])

    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    files_fetched = fetch_files(s3_uri, tmp_dir,
                                regexp=['url(ids|infos|links|inlinks|contents|contentsduplicate|_suggested_clusters|badlinks).txt.%d.gz' % part_id],
                                force_fetch=force_fetch)
    streams = {}

    path_local, fetched = files_fetched[0]
    for path_local, fetched in files_fetched:
        stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
        cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast

        if stream_identifier == "patterns":
            stream_patterns = cast(split_file(gzip.open(path_local)))
        else:
            streams[stream_identifier] = cast(split_file(gzip.open(path_local)))

    g = UrlDocumentGenerator(stream_patterns, **streams)

    docs = []
    for i, document in enumerate(g):
        document[1]['crawl_id'] = crawl_id
        document[1]['_id'] = '{}:{}'.format(crawl_id, document[0])
        docs.append(document[1])
        if i % 3000 == 2999:
            logger.info('%d items imported to urls_data ES for %s (part %d)' % (i, es_index, part_id))
            bulk(es, docs, doc_type=es_doc_type, index=es_index)
            docs = []
    # Push the missing documents
    if docs:
        bulk(es, docs, doc_type=es_doc_type, index=es_index)
