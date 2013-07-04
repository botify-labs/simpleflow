import os
import gzip

from pyelasticsearch import ElasticSearch, IndexAlreadyExistsError

from cdf.constants import URLS_DATA_MAPPING
from cdf.log import logger
from cdf.utils.s3 import fetch_files
from cdf.streams.caster import Caster
from cdf.streams.constants import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.url_data import UrlDataGenerator
from cdf.streams.utils import split_file


def prepare_crawl_index(crawL_id, es_location, es_index):
    es = ElasticSearch(es_location)
    try:
        es.create_index(es_index)
        es.put_mapping(es_index, 'urls_data', URLS_DATA_MAPPING)
    except IndexAlreadyExistsError:
        pass


def push_urls_to_elastic_search(crawl_id, part_id, s3_uri, es_location, es_index, tmp_dir_prefix='/tmp', force_fetch=False):
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

    es = ElasticSearch(es_location)

    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    files_fetched = fetch_files(s3_uri, tmp_dir, regexp=['url(ids|infos|links|contents).txt.%d.gz' % part_id], force_fetch=force_fetch)
    streams = {}

    path_local, fetched = files_fetched[0]
    for path_local, fetched in files_fetched:
        stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
        cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast

        if stream_identifier == "patterns":
            stream_patterns = cast(split_file(gzip.open(path_local)))
        else:
            streams[stream_identifier] = cast(split_file(gzip.open(path_local)))

    g = UrlDataGenerator(stream_patterns, **streams)

    docs = []
    for i, document in enumerate(g):
        docs.append(document[1])
        if i % 10000 == 9999:
            es.bulk_index(es_index, 'urls_data', docs)
            docs = []
            logger.info('%d items imported to urls_data ES for %s (part %d)' % (i, es_index, part_id))
    # Push the missing documents
    if docs:
        es.bulk_index(es_index, 'urls_data', docs)
