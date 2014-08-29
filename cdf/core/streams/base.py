from operator import itemgetter
from itertools import izip, chain, groupby, ifilter
import os
import gzip
import tempfile
import shutil

from cdf.core.streams.caster import Caster
from cdf.core.streams.utils import split_file
from cdf.core.constants import FIRST_PART_ID_SIZE, PART_ID_SIZE
from cdf.utils import s3
from cdf.utils.path import list_files, partition_aware_sort
from cdf.analysis.urls.utils import get_part_id
from cdf.query.constants import FIELD_RIGHTS


def is_s3_uri(uri):
    """Test if a uri represents a s3 uri"""
    return uri.startswith('s3')


# TODO(darkjh) separate `data_format` from StreamDef
# TODO(darkjh) separate document calculation from StreamDef
class StreamDefBase(object):
    """Abstract base class for all stream definitions

    Stream definition is the representation of a certain data source, that we
    will consume in the form of stream.
    It provides some static methods for constructing/persisting streams from/to
    some resource locations (eg. local disk path, s3 bucket etc.).

    To impl a concrete `StreamDef`, sub-classes need to specify some constants:
        `FILE`: the prefix of the document in the storage backend, the pattern
            is `${FILE}.txt.${PARTITION_NUM}.gz`

        `HEADERS`: data fields of the stream
              Ex : [('id', int), ('url', str), ('http_code', int)]

        `URL_DOCUMENT_MAPPING`: the mapping of the final document generated for
            a given url
    """

    @classmethod
    def field_idx(cls, field):
        """
        Return the field position of field in HEADERS
        """
        return map(lambda i: i[0], cls.HEADERS).index(field)

    # TODO(darkjh) no need to expose this in the interface
    #              client code can easily achieve this by using list comprehension
    @classmethod
    def fields_idx(cls, fields):
        """
        Return the positions of input fields in HEADERS
        :param fields: the list of input fields
        :type fields: iterable
        """
        return [cls.field_idx(field) for field in fields]

    # TODO(darkjh) how to handle reading of a particular file (regardless of the naming scheme)?
    #   - add a `load_from_file`
    #   - just disallow this usage
    # TODO(darkjh) load stream from s3 still need the caller to prepare a tmp_dir,
    #              use streaming s3 to resolve problem
    @classmethod
    def load(cls, uri, tmp_dir=None, part_id=None, force_fetch=False):
        """Load data stream from a data source location

        :param uri: uri to data source (local directory or s3 uri)
        :type uri: str
        :param tmp_dir: local tmp dir path, needed for loading stream from s3
        :type tmp_dir: str
        :param part_id: partition id, `None` for all existing partitions
        :type part_id: int
        :return: stream
        :rtype: stream
        """
        if is_s3_uri(uri):
            return cls._get_stream_from_s3(
                uri,
                tmp_dir=tmp_dir,
                part_id=part_id,
                force_fetch=force_fetch
            )
        else:
            if os.path.isdir(uri):
                return cls._get_stream_from_directory(
                    uri,
                    part_id=part_id
                )
            else:
                # TODO add a warning?
                return cls._get_stream_from_path(uri)

    @classmethod
    def load_path(cls, path, tmp_dir=None, force_fetch=False):
        """Load data stream from a specific path, regardless of resource
        naming scheme

        Use with care since it bypasses all our naming scheme.

        :param: path: path to data file
        :type path: str
        :param tmp_dir: local tmp dir path, needed for loading stream from s3
        :type tmp_dir: str
        :return: stream
        :rtype: stream
        """
        if is_s3_uri(path):
            return cls._get_stream_from_s3_path(
                path,
                tmp_dir=tmp_dir,
                force_fetch=force_fetch
            )
        else:
            return cls._get_stream_from_path(path)

    #TODO(darkjh) use pure streaming persist (key.set_contents_from_stream)
    @classmethod
    def persist(cls, stream, uri, part_id=None,
                first_part_size=FIRST_PART_ID_SIZE,
                part_size=PART_ID_SIZE):
        """Persist the contents of a stream

        :param stream: the stream to persist
        :type stream: iterator
        :param uri: data source location
        :type uri: str
        :param part_id: partition id the stream should be persisted into,
            Stream will be persist into partitions if it's set to `None`
        :type part_id: int
        :return: a list of persisted file paths
        :rtype: list
        """
        if is_s3_uri(uri):
            # s3 uri
            if part_id is None:
                # persist into partitions
                return cls._persist_to_s3(stream, uri, first_part_size, part_size)
            else:
                # persist into a partition
                return cls._persist_part_to_s3(stream, uri, part_id)
        else:
            # local path
            if part_id is None:
                # persist into partitions
                return cls._persist_all(stream, uri, first_part_size, part_size)
            else:
                # persist into a partition
                return cls._persist_part_id(stream, uri, part_id)

    @classmethod
    def _get_stream_from_directory(cls, directory, part_id=None):
        """Return a Stream instance from a directory

        It handles the case of partitions:
            - if a partition id is given, only stream that partition
            - if not, stream all partition in order

        :param directory: local directory containing partition files
        :type directory: str
        :param part_id: partition id, `None` by default
        :type part_id: int
        """
        pattern = r'{}\.txt\.{}\.gz'
        # partition-aware sort the files
        regexp = part_id if part_id is not None else '[0-9]+'
        regexp = pattern.format(cls.FILE, regexp)

        files = list_files(directory, regexp=regexp)
        streams = [cls._get_stream_from_path(f)
                   for f in partition_aware_sort(files)]

        return Stream(
            cls(),
            chain(*streams)
        )

    @classmethod
    def _get_stream_from_path(cls, path):
        """
        Return a Stream instance from a file path (the file must be gzip encoded)
        """
        cast = Caster(cls.HEADERS).cast
        if os.path.exists(path):
            iterator = cast(split_file(gzip.open(path)))
        else:
            iterator = iter([])
        return Stream(
            cls(),
            iterator
        )

    @classmethod
    def _get_stream_from_s3(cls, s3_uri, tmp_dir, part_id=None, force_fetch=False):
        """
        Return a Stream instance from a root storage uri.
        """
        if part_id is not None:
            regexp = '{}.txt.{}.gz'.format(cls.FILE, part_id)
        else:
            regexp = '{}.txt.([0-9]+).gz'.format(cls.FILE)
        s3.fetch_files(
            s3_uri,
            tmp_dir,
            regexp=regexp,
            force_fetch=force_fetch
        )
        return cls._get_stream_from_directory(tmp_dir, part_id)

    @classmethod
    def _get_stream_from_s3_path(cls, s3_uri_path, tmp_dir, force_fetch=False):
        """
        Return a Stream instance from a gzip file stored in S3
        """
        path, fetched = s3.fetch_file(
            s3_uri_path,
            os.path.join(tmp_dir, os.path.basename(s3_uri_path)),
            force_fetch=force_fetch
        )
        return cls._get_stream_from_path(path)

    # TODO(darkjh) used only in tests
    @classmethod
    def get_stream_from_iterator(cls, i):
        """
        Return a stream from an iterable object
        Warning : consider that the iterable object is already transformed
        It won't add missing/default values when necessary
        """
        cast = Caster(cls.HEADERS).cast
        return Stream(cls(), cast(i))

    @classmethod
    def _persist_all(cls, stream, directory,
                    first_part_id_size=FIRST_PART_ID_SIZE,
                    part_id_size=PART_ID_SIZE):
        """
        Persist a stream into a file located in a `directory`
        The filename will be automatically generated depending on the `StreamDef`'s stream and all `part_id`s found
        :return a list of files location
        """
        if not os.path.isdir(directory):
            raise Exception('{} must be a directory'.format(directory))

        files_generated = []
        for part_id, local_stream in groupby(stream, lambda k: get_part_id(k[0], first_part_id_size, part_id_size)):
            files_generated.append(cls._persist_part_id(local_stream, directory, part_id))
        return files_generated

    @classmethod
    def _persist_part_id(cls, stream, directory, part_id):
        """Persist the content of the stream into a partition file

        :return the file location where the stream has been stored
        """
        filename = os.path.join(directory, '{}.txt.{}.gz'.format(cls.FILE, part_id))
        f = gzip.open(filename, 'w')
        for entry in stream:
            f.write('\t'.join(str(k).encode('utf-8') for k in entry) + '\n')
        f.close()
        return filename

    @classmethod
    def _persist_to_s3(cls, stream, s3_uri,
                      first_part_id_size=FIRST_PART_ID_SIZE,
                      part_id_size=PART_ID_SIZE):
        """Persist the stream to s3 in partitions

        :param stream: stream to persist
        :param s3_uri: s3 uri
        :param first_part_id_size: size of first partition
        :param part_id_size: size of subsequent partitions
        :return: a list of s3 uris of persisted files
        """
        tmp_dir = tempfile.mkdtemp()
        local_files = cls._persist_all(stream, directory=tmp_dir,
                                      first_part_id_size=first_part_id_size,
                                      part_id_size=part_id_size)
        files = []
        for f in local_files:
            s3_file_path = os.path.join(s3_uri, os.path.basename(f))
            s3.push_file(s3_file_path, f)
            files.append(s3_file_path)
        shutil.rmtree(tmp_dir)
        return files

    @classmethod
    def _persist_part_to_s3(cls, stream, s3_uri, part_id):
        """Persist the stream to s3 as a single partition

        :param stream: stream to persist
        :param s3_uri: s3 uri
        :param part_id: partition id
        :return: the s3 uri of the persisted file on s3
        """
        tmp_dir = tempfile.mkdtemp()
        local_file = cls._persist_part_id(
            stream, directory=tmp_dir, part_id=part_id
        )
        s3_file_path = os.path.join(s3_uri, os.path.basename(local_file))
        s3.push_file(s3_file_path, local_file)
        shutil.rmtree(tmp_dir)
        return s3_file_path

    @classmethod
    def to_dict(cls, entry):
        """Return a dictionary from a stream entry

        Ex: (5, "http://www.site.com/", 200) will return
            {"id": 5, "url": "http://www.site.com/", "http_code": 200}
        """
        return {field[0]: value for field, value in izip(cls.HEADERS, entry)}

    @classmethod
    def create_temporary_dataset(cls):
        return TemporaryDataset(
            stream_def=cls
        )

    @classmethod
    def get_document_fields_from_options(self, options, remove_private=False):
        """
        Return the document fields enabled depending on options defined
        for the given feature
        :param options : a dictionnary with key as feature's identifier and sub-dict as value (k/v settings)
        :param remove_private : if PRIVATE flag is in field's `settings`, the field will be skipped
        """
        if not hasattr(self, 'URL_DOCUMENT_MAPPING'):
            return []
        fields = []
        for field, settings in self.URL_DOCUMENT_MAPPING.iteritems():
            if field.endswith('_exists') or FIELD_RIGHTS.PRIVATE in settings.get("settings", set()):
                continue
            if settings.get("enabled", lambda options: True)(options):
                fields.append(field)
        return fields


class Stream(object):
    """
    A Stream instance is the union of a StreamDefBase instance and an iterable
    """

    def __init__(self, stream_def, iterator):
        """
        :param stream_def : A StreamDefBase's subclass instance
        :param iterator : an iterator
        """
        self.stream_def = stream_def
        self.iterator = iterator
        self._has_filters = False
        self._filters = []

    def __iter__(self):
        return self

    def __repr__(self):
        return '<Stream of %s>' % self.stream_def.__class__.__name__

    def next(self):
        if not self._has_filters:
            return self.iterator.next()
        return self._filtered_iterator.next()

    def add_filter(self, field, func):
        """
        Apply a filter func to a field
        Ex : self.add_filter('http_code', lambda i: i == '200')
        """
        self._has_filters = True
        self._filters.append((self.stream_def.field_idx(field), func))
        self._filtered_iterator = ifilter(
            lambda v: all(func(v[field_idx]) for field_idx, func in self._filters),
            self.iterator
        )


class TemporaryDataset(object):
    """
    Store a given dataset to a temporary list
    Each item appended to the dataset (with TemporaryDataset.append) must be compat with the `StreamDef` instance headers
    The list can be persisted by calling TemporaryDataset.persist or TemporaryDataset.persist_to_s3

    Warning : You need to ensure that the temporary dataset can fit in memory
    """
    def __init__(self, stream_def):
        self.stream_def = stream_def
        self.dataset = []

    def append(self, *args):
        """
        Append an entry to the dataset
        Types and value of arguments must fit with self.stream_def.HEADERS definition
        (for speed reasons, we don't check here the validity)
        """
        self.dataset.append(args)

    def sort(self, idx=0):
        """
        Sort the dataset depending of a given `idx`
        """
        self.dataset = sorted(self.dataset, key=itemgetter(0))

    def persist(self, uri,
                first_part_size=FIRST_PART_ID_SIZE,
                part_size=PART_ID_SIZE,
                sort=True):
        if sort:
            self.sort()
        return self.stream_def.persist(
            stream=iter(self.dataset),
            uri=uri,
            first_part_size=first_part_size,
            part_size=part_size
        )