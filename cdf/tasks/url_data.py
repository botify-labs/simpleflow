import os
import gzip
import itertools
import lz4

from elasticsearch import Elasticsearch

from cdf.constants import URLS_DATA_MAPPING
from cdf.log import logger
from cdf.utils.s3 import fetch_files
from cdf.utils.es import bulk
from cdf.streams.caster import Caster
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.urls.generators.documents import UrlDocumentGenerator
from cdf.collections.urls.transducers.metadata_duplicate import get_duplicate_metadata
from cdf.streams.utils import split_file, split
from cdf.utils.remote_files import nb_parts_from_crawl_location


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

    files_fetched = fetch_files(s3_uri, tmp_dir, regexp=['url(ids|infos|links|inlinks|contents).txt.%d.gz' % part_id, 'url_suggested_clusters.%d.txt.lz4' % part_id], force_fetch=force_fetch)
    streams = {}

    path_local, fetched = files_fetched[0]
    for path_local, fetched in files_fetched:
        stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
        cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast

        if stream_identifier == "patterns":
            stream_patterns = cast(split_file(gzip.open(path_local)))
        elif stream_identifier == "suggest":
            file_content = lz4.loads(open(path_local).read())
            if not file_content:
                streams["suggest"] = iter([])
            else:
                cast = Caster(STREAMS_HEADERS["SUGGEST"]).cast
                streams["suggest"] = cast(split(file_content.split('\n')))
        else:
            streams[stream_identifier] = cast(split_file(gzip.open(path_local)))

    g = UrlDocumentGenerator(stream_patterns, **streams)

    docs = []
    for i, document in enumerate(g):
        document[1]['crawl_id'] = crawl_id
        document[1]['_id'] = '{}:{}'.format(crawl_id, document[0])
        docs.append(document[1])
        if i % 3000 == 2999:
            bulk(es, docs, doc_type=es_doc_type, index=es_index)
            docs = []
            logger.info('%d items imported to urls_data ES for %s (part %d)' % (i, es_index, part_id))
    # Push the missing documents
    if docs:
        bulk(es, docs, doc_type=es_doc_type, index=es_index)


def make_metadata_duplicates_file(crawl_id, s3_uri, first_part_id_size, part_id_size, tmp_dir_prefix='/tmp', force_fetch=False):
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

    current_part_id = 0
    f = gzip.open(os.path.join(tmp_dir, 'urlcontentsduplicate.txt.0.gz'), 'w')
    for i, (url_id, metadata_type, filled_nb, duplicates_nb, is_first, target_urls_ids) in enumerate(generator):

        # check the part id
        if (current_part_id == 0 and url_id > first_part_id_size) or \
           (current_part_id > 0 and (url_id - first_part_id_size) / part_id_size != current_part_id - 1):
            f.close()
            current_part_id += 1
            f = gzip.open(os.path.join(tmp_dir, 'urlcontentsduplicate.txt.{}.gz'.format(current_part_id)), 'w')

        f.write('\t'.join((
            str(url_id),
            str(metadata_type),
            str(filled_nb),
            str(duplicates_nb),
            '1' if is_first else '0',
            ';'.join(str(u) for u in target_urls_ids)
        )) + '\n')
    f.close()


def push_metadata_duplicate_to_elastic_search(crawl_id, s3_uri, es_location, es_index, es_doc_type, tmp_dir_prefix='/tmp', force_fetch=False):
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

    host, port = es_location[7:].split(':')
    es = Elasticsearch([{'host': host, 'port': int(port)}])
    docs = []
    for i, (metadata_type, url_id, duplicates_nb, is_first, target_urls_ids) in enumerate(generator):
        doc = {
            "_id": '{}:{}'.format(crawl_id, url_id),
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
            bulk(es, docs, doc_type=es_doc_type, index=es_index, bulk_type="update")
            docs = []
            logger.info('%d items updated to crawl_%d ES for %s' % (i, crawl_id, es_index))
    if docs:
        bulk(es, docs, doc_type=es_doc_type, index=es_index, bulk_type="update")
