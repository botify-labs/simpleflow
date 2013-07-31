import os
import gzip
import itertools

from pyelasticsearch import ElasticSearch, ElasticHttpError, IndexAlreadyExistsError

from cdf.constants import URLS_DATA_MAPPING
from cdf.log import logger
from cdf.utils.s3 import fetch_files
from cdf.streams.caster import Caster
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.urls.generators.documents import UrlDocumentGenerator
from cdf.collections.urls.transducers.metadata_duplicate import get_duplicate_metadata
from cdf.streams.utils import split_file
from cdf.utils.remote_files import nb_parts_from_crawl_location


def remove_crawl_doctype(crawl_id, es_location, es_index):
    es = ElasticSearch(es_location)
    try:
        es.delete_all(es_index, "crawl_{}".format(crawl_id))
    except Exception, e:
        logger.error("{} : {}".format(type(e), str(e)))


def prepare_crawl_index(crawl_id, es_location, es_index):
    es = ElasticSearch(es_location)
    try:
        es.create_index(es_index)
    except (ElasticHttpError, IndexAlreadyExistsError), e:
        logger.error("{} : {}".format(type(e), str(e)))
    es.put_mapping(es_index, 'crawl_%d' % crawl_id, URLS_DATA_MAPPING)


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

    files_fetched = fetch_files(s3_uri, tmp_dir, regexp=['url(ids|infos|links|inlinks|contents).txt.%d.gz' % part_id], force_fetch=force_fetch)
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
        docs.append(document[1])
        if i % 3000 == 2999:
            es.bulk_index(es_index, 'crawl_%d' % crawl_id, docs)
            docs = []
            logger.info('%d items imported to urls_data ES for %s (part %d)' % (i, es_index, part_id))
    # Push the missing documents
    if docs:
        es.bulk_index(es_index, 'crawl_%d' % crawl_id, docs)


def push_metadata_duplicate_to_elastic_search(crawl_id, s3_uri, es_location, es_index, tmp_dir_prefix='/tmp', force_fetch=False):
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    streams_types = {'patterns': [],
                     'contents': []
                     }

    for part_id in xrange(0, nb_parts_from_crawl_location(s3_uri)):
        files_fetched = fetch_files(s3_uri,
                                    tmp_dir,
                                    regexp='url(ids|contents).txt.%d.gz' % part_id,
                                    force_fetch=force_fetch)

        for path_local, fetched in files_fetched:
            stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
            cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
            streams_types[stream_identifier].append(cast(split_file(gzip.open(path_local))))

    generator = get_duplicate_metadata(itertools.chain(*streams_types['patterns']),
                                       itertools.chain(*streams_types['contents']))

    es = ElasticSearch(es_location)
    docs = []
    for i, (metadata_type, url_id, duplicates_nb, is_first, target_urls_ids) in enumerate(generator):
        doc = {
            "id": url_id,
            "script": """if(ctx._source['metadata_duplicate'] == null) {{
                            ctx._source['metadata_duplicate_nb'] = {{ '{metadata_type}':duplicate_nb }};
                            ctx._source['metadata_duplicate'] = {{ '{metadata_type}':urls_ids }};
                         }} else {{
                            ctx._source.metadata_duplicate_nb['{metadata_type}'] = duplicate_nb;
                            ctx._source.metadata_duplicate['{metadata_type}'] = urls_ids;
                          }}
                      if(is_first == true) {{
                          if(ctx._source['metadata_duplicate_is_first'] == null) {{
                              ctx._source['metadata_duplicate_is_first'] = {{ '{metadata_type}': true }};
                          }} else {{
                              ctx._source.metadata_duplicate_is_first['{metadata_type}'] = true;
                          }}
                      }}
                      """.format(metadata_type=metadata_type),
            "params": {
                "urls_ids": target_urls_ids,
                "duplicate_nb": duplicates_nb,
                "is_first": is_first
            }
        }
        docs.append(doc)
        if i % 10000 == 9999:
            es.bulk_update(es_index, 'crawl_%d' % crawl_id, docs)
            docs = []
            logger.info('%d items updated to crawl_%d ES for %s' % (i, crawl_id, es_index))
    if docs:
        es.bulk_update(es_index, 'crawl_%d' % crawl_id, docs)
