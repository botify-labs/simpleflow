import os
import ujson as json
import time

from cdf.utils.kvstore import LevelDB
from cdf.features.comparison.constants import SEPARATOR
from cdf.features.comparison.exceptions import UrlKeyDecodingError


_HASH_KEY = 'url_hash'
_PREV_KEY = 'previous'
_PREV_EXISTS_KEY = 'previous_exists'
_CRAWL_ID_KEY = 'crawl_id'
_URL_ID_KEY = 'url_id'

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
        doc = json.loads(raw_doc)
        uid = doc['id']
        url = doc['url']
        url_key = encode_url_key(url, uid)
        yield url_key, raw_doc


def load_documents_db(document_stream, tmp_dirpath,
                      buffer_size=_BUFFER_SIZE):
    """Bulk load documents to local KVStore

    :param document_stream: input key, value document stream
    :param tmp_dirpath: temp dir for KVStore
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