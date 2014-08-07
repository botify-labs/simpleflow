import os
import tempfile
import shutil
import gzip
import ujson as json
import copy

from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir
from cdf.core.decorators import feature_enabled
from cdf.features.comparison import logger
from cdf.features.comparison.matching import (
    document_match, load_documents_db,
    load_raw_documents, _LEVELDB_BLOCK_SIZE,
    document_merge, generate_conversion_table,
    document_url_id_correction)
from cdf.features.comparison.constants import (
    MATCHED_FILE_PATTERN,
    COMPARISON_PATH, EXTRA_FIELDS_FORMAT)
from cdf.utils.s3 import fetch_file, push_file, stream_files
from cdf.utils.path import makedirs

_FEATURE_ID = 'comparison'
_REF_PATH = 'compare_ref'
_NEW_PATH = 'compare_new'
_DOC_FILE_REGEXP = 'url_documents.json.*.gz'



def _get_max_crawled_id(s3_uri):
    """Get the max crawled url id by parsing the metadata file

    Suggestion: add/enhance our app web service so that program
        can query crawl information in a RESTful way
        like `GET staging/crawls/1149/max_crawl_id`
    """
    tmp_dir = tempfile.mkdtemp()
    global_crawl_info_filename = "files.json"
    fetch_file(os.path.join(s3_uri, global_crawl_info_filename),
               os.path.join(tmp_dir, global_crawl_info_filename),
               force_fetch=True)
    with open(os.path.join(tmp_dir, global_crawl_info_filename)) as f:
        info = json.load(f)
    shutil.rmtree(tmp_dir)
    return info['max_uid_we_crawled']


def _key_stream_from_db(db):
    with db.iterator() as db_iter:
        for key, _ in db_iter:
            yield key


def _doc_stream_from_db(db):
    with db.iterator() as db_iter:
        for _, doc_str in db_iter:
            yield json.loads(doc_str)


def _get_document_path(crawl_path):
    return os.path.join(crawl_path, 'documents')


def _create_partition_file(path, file_pattern, partition_nb):
    file_name = os.path.join(path, file_pattern.format(partition_nb))
    return gzip.open(file_name, 'w')


def _get_comparison_document_uri(documents_uri):
    return os.path.join(documents_uri, COMPARISON_PATH)


@with_temporary_dir
@feature_enabled(_FEATURE_ID)
def match_documents(ref_s3_uri, new_s3_uri, new_crawl_id,
                    tmp_dir=None, force_fetch=False,
                    part_size=300000):
    """Match documents representing the same url in two crawls

    It's a pre-processing step for crawl comparison feature. This task
    analyses both the newly crawled documents and the reference dataset
    and merge the matching url documents together. It outputs a merged
    url document partition dataset including the union of both dataset.

    Note that the `url_id`s in the reference documents are corrected to
    be coherent within the new crawl's context.

    :param ref_s3_uri: reference crawl s3 uri
    :param new_s3_uri: new crawl s3 uri
    :param tmp_dir: local working directory
    :param force_fetch: not useful here since documents are streamed
    :param part_size: size of each output partition. Note that since output
        of this task is the final documents to push, so no need to maintain
        the initial partition scheme.
    """
    # Create local working directories under `tmp_dir`
    ref_path = os.path.join(tmp_dir, _REF_PATH)
    new_path = os.path.join(tmp_dir, _NEW_PATH)
    makedirs(ref_path)
    makedirs(new_path)

    # Fetch document dataset streams
    #   raw document stream -> filter/clean (SKIPPED now) -> load DB
    # Reference crawl documents
    ref_raw_stream = stream_files(_get_document_path(ref_s3_uri),
                                  _DOC_FILE_REGEXP)
    ref_doc_stream = load_raw_documents(ref_raw_stream)
    ref_db = load_documents_db(ref_doc_stream, ref_path)

    # New crawl documents
    new_raw_stream = stream_files(_get_document_path(new_s3_uri),
                                  _DOC_FILE_REGEXP)
    new_doc_stream = load_raw_documents(new_raw_stream)
    new_db = load_documents_db(new_doc_stream, new_path)

    logger.info('Finished DB loading ...')

    # Output to partition files
    matched_file_list = []
    matched_file = _create_partition_file(
        new_path, MATCHED_FILE_PATTERN, 0)
    matched_file_list.append(matched_file.filename)

    # reopen the DBs with read options
    ref_db.reopen(block_size=_LEVELDB_BLOCK_SIZE)
    new_db.reopen(block_size=_LEVELDB_BLOCK_SIZE)

    # 1. Generate url_id conversion table
    # DB key stream
    ref_key_stream = _key_stream_from_db(ref_db)
    new_key_stream = _key_stream_from_db(new_db)

    conversion_table = generate_conversion_table(
        ref_key_stream, new_key_stream)

    # 2. Match documents
    # DB iterators
    ref_stream = document_url_id_correction(
        _doc_stream_from_db(ref_db),
        conversion_table=conversion_table)
    new_stream = _doc_stream_from_db(new_db)

    # construct the final stream
    merged_stream = document_merge(
        document_match(ref_stream, new_stream), new_crawl_id)

    matched_count = 0
    # Write into partition files
    for doc in merged_stream:
        matched_count += 1
        if matched_count % part_size == 0 and matched_count > 0:
            # if a new partition fitle is needed
            matched_file.close()
            matched_file = _create_partition_file(
                new_path, MATCHED_FILE_PATTERN,
                matched_count / part_size)
            matched_file_list.append(matched_file.filename)

        matched_file.write(json.dumps(doc) + '\n')

    # Close opened files
    matched_file.close()
    logger.info('Matching finished ...')

    # Pushing results
    # Pushing `matched` dataset
    comparison_uri = os.path.join(_get_document_path(new_s3_uri),
                                  COMPARISON_PATH)
    for matched_file in matched_file_list:
        base_name = os.path.basename(matched_file)
        push_file(
            os.path.join(comparison_uri, base_name),
            matched_file
        )

    # Destroy the temporary DB
    ref_db.destroy()
    new_db.destroy()


def get_comparison_data_format(data_format, extras=EXTRA_FIELDS_FORMAT):
    """Prepare ElasticSearch mapping for comparison feature

    :param data_format: original internal data format
    :param extras: extra fields to be added
    :return: mutated data_format for comparison
    """
    previous_format = {
        'previous.' + k: v for k, v in data_format.iteritems()
    }
    format = copy.deepcopy(data_format)
    format.update(previous_format)
    format.update(extras)

    return format