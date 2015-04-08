import os
import time

from enum import Enum

from cdf.utils import safe_json

from cdf.metadata.url.backend import ELASTICSEARCH_BACKEND
from cdf.utils.kvstore import LevelDB
from cdf.features.comparison import logger
from cdf.features.comparison.constants import SEPARATOR, MatchingState
from cdf.features.comparison.exceptions import (
    UrlKeyDecodingError,
    UrlIdFieldFormatError
)
from cdf.metadata.url.url_metadata import URL_ID
from cdf.utils.dict import (
    path_in_dict,
    get_subdict_from_path,
    update_path_in_dict
)


_HASH_KEY = 'url_hash'
_PREV_KEY = 'previous'
_PREV_EXISTS_KEY = 'previous_exists'
_CRAWL_ID_KEY = 'crawl_id'
_URL_ID_KEY = 'url_id'
_DIFF_KEY = 'diff'

_DISAPPEARED_URL_ID = -1

_LEVELDB_DIR_NAME = 'tmpdb'
_LEVELDB_WRITE_BUFFER = 256 * 1024 * 1024  # 256M
_LEVELDB_BLOCK_SIZE = 256 * 1024  # 256K

_BUFFER_SIZE = 10000
_COMPACTION_WAIT = 10  # wait 10s after bulk loading


def _get_db_path(path, name=_LEVELDB_DIR_NAME):
    return os.path.join(path, name)


def encode_url_key(url, uid, separator=SEPARATOR):
    return url + separator + str(uid)


def decode_url_key(url_key, separator=SEPARATOR):
    splt = url_key.split(separator)
    _len = len(splt)
    if _len == 2:
        url = splt[0]
        uid_str = splt[1]
    elif _len > 2:
        uid_str = splt.pop()
        url = separator.join(splt)
    else:
        raise UrlKeyDecodingError(url_key)

    try:
        uid = int(uid_str)
    except ValueError:
        raise UrlKeyDecodingError(url_key)

    return url, uid


def load_raw_documents(raw_document_stream):
    """Decode/loads a stream of raw url document strings into a
    key value stream

    The input should be json-parsable string.

    :param raw_document_stream: raw string document stream
    :return: a generator of (key, document)
    """
    for raw_doc in raw_document_stream:
        doc = safe_json.loads(raw_doc)
        uid = doc['id']
        url = doc['url']
        url_key = encode_url_key(url, uid)
        yield url_key, raw_doc


def load_documents_db(document_stream, tmp_dirpath,
                      buffer_size=_BUFFER_SIZE):
    """Bulk load documents to local KVStore

    :param document_stream: input key, value document stream
    :param tmp_dirpath: temp parent directory for KVStore
    :param buffer_size: the write buffer size
    :return: the KVStore handler
    """
    # create a levelDB instance
    db_path = _get_db_path(tmp_dirpath)
    db = LevelDB(db_path)
    db.open(write_buffer_size=_LEVELDB_WRITE_BUFFER,
            block_size=_LEVELDB_BLOCK_SIZE)

    # batch write to DB
    db.batch_write(document_stream, batch_size=buffer_size)

    # let levelDB do compaction
    time.sleep(_COMPACTION_WAIT)
    return db


def generate_conversion_table(ref_key_stream, new_key_stream):
    """Generate a conversion table out of two crawls' url key stream

    Url keys encode url and their url_id together, this avoids decoding
    document json structure

    :param ref_key_stream: url key stream of the reference crawl
    :param new_key_stream: url key stream of the new crawl
    :return: a conversion table between old url_id and new url_id
    :rtype: dict
    """
    ref_url, ref_uid = decode_url_key(next(ref_key_stream))
    new_url, new_uid = decode_url_key(next(new_key_stream))

    count = 0
    conversion_table = dict()

    # Merge-sort style merge
    while True:
        # match
        if new_url == ref_url:
            count += 1
            if count % 1000 == 0 and count > 0:
                logger.info('Process {} document pairs '
                            'for conversion table ...'.format(count))

            # ref_url_id -> new_url_id
            conversion_table[ref_uid] = new_uid

            # advances both cursor
            try:
                ref_url, ref_uid = decode_url_key(next(ref_key_stream))
            except StopIteration:
                break
            try:
                new_url, new_uid = decode_url_key(next(new_key_stream))
            except StopIteration:
                break

        elif new_url > ref_url:
            # no match
            # advances ref dataset cursor
            try:
                ref_url, ref_uid = decode_url_key(next(ref_key_stream))
            except StopIteration:
                break
        else:
            # no match
            # advances new dataset cursor
            try:
                new_url, new_uid = decode_url_key(next(new_key_stream))
            except StopIteration:
                break

    return conversion_table


class _FieldFormat(Enum):
    SEQ = 1
    SEQ_SEQ = 2
    NUMBER = 3
    DICT = 4
    UNKNOWN = 5


def _sniff_url_id_format(field_path, field_value):
    if isinstance(field_value, (list, tuple)):
        if len(field_value) > 0:
            # peek the first value
            first = field_value[0]
            if isinstance(first, (int, long)):
                return _FieldFormat.SEQ
            elif isinstance(field_value, (list, tuple)):
                return _FieldFormat.SEQ_SEQ
            else:
                raise UrlIdFieldFormatError(field_path, field_value)
        else:
            # can't decide on empty sequence
            # wait for another document to decide
            return _FieldFormat.UNKNOWN
    elif isinstance(field_value, (int, long)):
        return _FieldFormat.NUMBER
    elif isinstance(field_value, dict):
        return _FieldFormat.DICT
    else:
        raise UrlIdFieldFormatError(field_path, field_value)


def _collect_url_id(field_format, field_value):
    """Collects the `url_id` according to its format into a set

    :param field_format: `field_value`'s format
    :param field_value: the url_id related field's value
    :return: collected `url_id`s, de-duplicated
    :rtype: set
    """
    if field_format is _FieldFormat.NUMBER:
        return {field_value}
    elif field_format is _FieldFormat.SEQ:
        return set(field_value)
    elif field_format is _FieldFormat.SEQ_SEQ:
        return set(map(lambda i: i[0], field_value))
    elif field_format is _FieldFormat.DICT:
        if _URL_ID_KEY in field_value:
            return {field_value[_URL_ID_KEY]}
        else:
            return set()
    else:
        return set()


def _correct_url_id(field_format, lookup, field_value,
                    field_path, document):
    if field_format is _FieldFormat.NUMBER:
        update = lookup[field_value]
    elif field_format is _FieldFormat.SEQ:
        update = map(lambda i: lookup[i], field_value)
    elif field_format is _FieldFormat.SEQ_SEQ:
        update = map(lambda i: [lookup[i[0]]] + i[1:], field_value)
    elif field_format is _FieldFormat.DICT:
        if _URL_ID_KEY in field_value:
            new_url_id = lookup[field_value[_URL_ID_KEY]]
            update = {_URL_ID_KEY: new_url_id}
        else:
            return
    else:
        return

    update_path_in_dict(field_path, update, document)


def _get_url_id_fields(data_backend):
    fields = []
    for field, value in data_backend.iteritems():
        if 'settings' in value and URL_ID in value['settings']:
            fields.append(field)

    return fields


# Fields that are actually stored as `url_id`
# so they need correction
_CORRECTION_FIELDS = _get_url_id_fields(ELASTICSEARCH_BACKEND.data_format)


def document_url_id_correction(document_stream,
                               conversion_table,
                               correction_fields=_CORRECTION_FIELDS):
    """Correct all url_id fields in the document_stream by using the
    conversion_table

    :param document_stream: stream of decoded url documents (dict)
    :param conversion_table: old url_id to new url_id lookup table
    :param correction_fields: url_list fields
    :return: generator of corrected url documents
    :rtype: stream of dict
    """
    # sniff and memorize a `url_id` related field's format
    #   eg. it's a list of tuple or simply an integer
    url_id_format = dict()

    for ref_doc in document_stream:
        # memorize document url_id related fields' accesses
        field_access = dict()
        # local lookup
        #   1. conversion table's conversion
        #   2. inverse `url_id` for no-match urls
        lookup = dict()
        old_ids = set()

        # collect (and sniff format if not done yet)
        for field_path in correction_fields:
            if path_in_dict(field_path, ref_doc):
                field_value = get_subdict_from_path(field_path, ref_doc)

                # sniff, if necessary
                if field_path not in url_id_format:
                    field_format = _sniff_url_id_format(
                        field_access, field_value)
                    if field_format is not _FieldFormat.UNKNOWN:
                        url_id_format[field_path] = field_format

                field_access[field_path] = field_value

                # collect
                if field_path in url_id_format:
                    old_ids.update(_collect_url_id(
                        url_id_format[field_path], field_value))

        # lookup
        for _id in old_ids:
            if _id in conversion_table:
                # matched url
                new_id = conversion_table[_id]
            else:
                # no-match (disappeared) url
                new_id = -_id
            lookup[_id] = new_id

        # correction
        for field_path, field_value in field_access.iteritems():
            if field_path in url_id_format:
                _correct_url_id(url_id_format[field_path], lookup,
                                field_value, field_path, ref_doc)

        yield ref_doc


def document_match(ref_doc_stream, new_doc_stream):
    """Match url documents between the reference crawl and the new crawl,
    returns the matched documents and their matching status

    The input and output document is python dictionary.

    :param ref_doc_stream: the reference crawl's document stream, the stream
        should be sorted on the `url` string of each document
    :param new_doc_stream: the new crawl's document stream, the stream should
        be sorted sorted on the `url` string of each document

    :return: the merged document, in 3 cases:
        1. matched url returns
            MATCH, (ref document, new document)
        2. newly discovered url
            DISCOVER, (None, new document)
        3. disappeared url
            DISAPPEAR, (ref document, None)
    """
    # Iterator ended markers
    ref_ended = False
    new_ended = False

    new_doc = next(new_doc_stream)
    new_url = new_doc['url']
    ref_doc = next(ref_doc_stream)
    ref_url = ref_doc['url']

    count = 0
    # Merge-sort style merge
    while True:
        # 3 cases
        if new_url == ref_url:
            count += 1
            if count % 1000 == 0 and count > 0:
                logger.info('Matched {} document pairs ...'.format(count))
            yield MatchingState.MATCH, (ref_doc, new_doc)

            # advances both cursor
            try:
                ref_doc = next(ref_doc_stream)
                ref_url = ref_doc['url']
            except StopIteration:
                logger.info('Reference dataset ended ...')
                ref_doc = None
                new_doc = None
                ref_ended = True
                break
            try:
                new_doc = next(new_doc_stream)
                new_url = new_doc['url']
            except StopIteration:
                logger.info('New dataset ended ...')
                ref_doc = None
                new_doc = None
                new_ended = True
                break

        elif new_url > ref_url:
            # `disappeared` url
            yield MatchingState.DISAPPEAR, (ref_doc, None)

            # advances ref dataset cursor
            try:
                ref_doc = next(ref_doc_stream)
                ref_url = ref_doc['url']
            except StopIteration:
                logger.info('Reference dataset ended ...')
                ref_ended = True
                break
        else:
            # new crawled url
            yield MatchingState.DISCOVER, (None, new_doc)
            # advances new dataset cursor
            try:
                new_doc = next(new_doc_stream)
                new_url = new_doc['url']
            except StopIteration:
                logger.info('New dataset ended ...')
                new_ended = True
                break

    logger.info("Matched {} documents in total ...".format(count))

    if ref_ended:
        # write remaining new dataset document
        if new_doc:
            yield MatchingState.DISCOVER, (None, new_doc)
        for new_doc in new_doc_stream:
            yield MatchingState.DISCOVER, (None, new_doc)
    elif new_ended:
        # write remaining reference dataset document
        if ref_doc:
            yield MatchingState.DISAPPEAR, (ref_doc, None)
        for ref_doc in ref_doc_stream:
            yield MatchingState.DISAPPEAR, (ref_doc, None)


def document_merge(matching_stream, new_crawl_id):
    """Merge matched url documents together

    :param matching_stream: matched documents stream
    :type matching_stream: MatchingStatus, (dict, dict)
    :param new_crawl_id: crawl_id of the new crawl
    :type new_crawl_id: int
    :return: generator of merged document
    :rtype: dict
    """
    result_doc = None
    for state, (ref_doc, new_doc, diff_doc) in matching_stream:
        if state is MatchingState.MATCH:
            # remove `_id` field in ref doc if it's present
            if '_id' in ref_doc:
                del ref_doc['_id']
            # merged the reference document
            result_doc = new_doc
            result_doc[_PREV_KEY] = ref_doc
            result_doc[_PREV_EXISTS_KEY] = True
            if diff_doc is not None:
                result_doc[_DIFF_KEY] = diff_doc
        elif state is MatchingState.DISAPPEAR:
            # correct the `crawl_id`
            result_doc = ref_doc
            result_doc[_CRAWL_ID_KEY] = new_crawl_id
            # correct the `_id`
            result_doc['_id'] = '{}:{}'.format(
                new_crawl_id, result_doc['id'])
            # add disappeared marker/flag
            result_doc['disappeared'] = True
        elif state is MatchingState.DISCOVER:
            # no special processing for now
            result_doc = new_doc
        else:
            # should not happen
            logger.warn("Wrong matching status ignored: "
                        "{}".format(state))
            continue

        yield result_doc