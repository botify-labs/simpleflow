import os
import gzip

from elasticsearch import Elasticsearch

from cdf.log import logger
from cdf.utils.s3 import fetch_files
from cdf.utils.es import bulk
from cdf.core.streams.caster import Caster
from cdf.metadata.raw import STREAMS_HEADERS, STREAMS_FILES
from cdf.core.analysis.urls.generators.documents import UrlDocumentGenerator
from cdf.core.streams.utils import split_file
from .decorators import TemporaryDirTask as with_temporary_dir
from .constants import DEFAULT_FORCE_FETCH, ES_MAPPING


def prepare_crawl_index(crawl_id, es_location, es_index, es_doc_type):
    host, port = es_location[7:].split(':')
    es = Elasticsearch([{'host': host, 'port': int(port)}])
    try:
        es.indices.create(es_index)
    except Exception, e:
        logger.error("{} : {}".format(type(e), str(e)))
    es.indices.put_mapping(es_index, es_doc_type, ES_MAPPING)


@with_temporary_dir
def push_urls_to_elastic_search(crawl_id, part_id, s3_uri, es_location, es_index, es_doc_type, tmp_dir=None, force_fetch=DEFAULT_FORCE_FETCH):
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
    files_fetched = fetch_files(s3_uri, tmp_dir,
                                regexp=['url(ids|infos|links|inlinks|contents|' +
                                        'contentsduplicate|_suggested_clusters|' +
                                        'badlinks).txt.%d.gz' % part_id],
                                force_fetch=force_fetch)
    streams = {}

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
