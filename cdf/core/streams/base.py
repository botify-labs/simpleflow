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
from cdf.utils.path import get_files_ordered_by_part_id
from cdf.analysis.urls.utils import get_part_id


class StreamDefBase(object):
    """
    StremDefBase is an abstractable object waiting for some constants :
    FILE : the prefix of the document in the storage backend (ex : urlids, urlcontents...)
    HEADERS : the definition of the streams fields
              Ex : [('id', int), ('url', str), ('http_code', int)
    URL_DOCUMENT_MAPPING : the mapping of the final document generated for a given url
    """

    @classmethod
    def field_idx(cls, field):
        """
        Return the field position of field in HEADERS
        """
        return map(lambda i: i[0], cls.HEADERS).index(field)

    @classmethod
    def get_stream_from_directory(cls, directory, part_id=None):
        """
        Return a Stream instance from a directory
        """
        if part_id is not None:
            return cls.get_stream_from_path(os.path.join(directory, "{}.txt.{}.gz".format(cls.FILE, part_id)))

        # We fetch all files and we sort them by part_id
        # We don't only sort on os.listdir, because 'file.9.txt' > 'file.10.txt'
        streams = []
        for f in get_files_ordered_by_part_id(directory, cls.FILE):
            streams.append(cls.get_stream_from_path(os.path.join(directory, f)))

        return Stream(
            cls(),
            chain(*streams)
        )

    @classmethod
    def get_stream_from_path(cls, path):
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
    def get_stream_from_s3(cls, s3_uri, tmp_dir, part_id=None, force_fetch=False):
        """
        Return a Stream instance from a root storage uri.
        """
        if part_id:
            regexp = '{}.txt.{}.gz'.format(cls.FILE, part_id)
        else:
            regexp = '{}.txt.([0-9]+).gz'.format(cls.FILE)
        s3.fetch_files(
            s3_uri,
            tmp_dir,
            regexp=regexp,
            force_fetch=force_fetch
        )
        return cls.get_stream_from_directory(tmp_dir, part_id)

    @classmethod
    def get_stream_from_s3_path(cls, s3_uri_path, tmp_dir, force_fetch=False):
        """
        Return a Stream instance from a gzip file stored in S3
        """
        path, fetched = s3.fetch_file(
            s3_uri_path,
            os.path.join(tmp_dir, os.path.basename(s3_uri_path)),
            force_fetch=force_fetch
        )
        return cls.get_stream_from_path(path)

    @classmethod
    def get_stream_from_file(cls, f):
        """
        Return a stream from a `file` instance
        """
        cast = Caster(cls.HEADERS).cast
        return Stream(
            cls(),
            cast(split_file(f))
        )

    @classmethod
    def get_stream_from_iterator(cls, i):
        """
        Return a stream from an iterable object
        Warning : consider that the iterable object is already transformed
        It won't add missing/default values when necessary
        """
        return Stream(cls(), i)

    def persist(self, stream, directory, first_part_id_size=FIRST_PART_ID_SIZE, part_id_size=PART_ID_SIZE):
        """
        Persist a stream into a file located in a `directory`
        The filename will be automatically generated depending on the `StreamDef`'s stream and all `part_id`s found
        :return a list of files location
        """
        if not os.path.isdir(directory):
            raise Exception('{} must be a directory'.format(directory))

        files_generated = []
        for part_id, local_stream in groupby(stream, lambda k: get_part_id(k[0], first_part_id_size, part_id_size)):
            files_generated.append(self._persist_part_id(local_stream, directory, part_id, first_part_id_size, part_id_size))
        return files_generated

    def _persist_part_id(self, stream, directory, part_id, first_part_id_size=FIRST_PART_ID_SIZE, part_id_size=PART_ID_SIZE):
        """
        Persist a stream into a file located in a `directory`
        The filename will be automatically generated depending on the `StreamDef`'s stream and the given `part_id`
        :return the file location where the stream has been stored
        """
        filename = os.path.join(directory, '{}.txt.{}.gz'.format(self.FILE, part_id))
        self.f = gzip.open(filename, 'w')
        for entry in stream:
            self.f.write('\t'.join(str(k).encode('utf-8') for k in entry) + '\n')
        self.f.close()
        return filename

    def persist_to_s3(self, stream, s3_uri, first_part_id_size=FIRST_PART_ID_SIZE, part_id_size=PART_ID_SIZE):
        tmp_dir = tempfile.mkdtemp()
        local_files = self.persist(stream, directory=tmp_dir, first_part_id_size=first_part_id_size, part_id_size=part_id_size)
        files = []
        for f in local_files:
            s3.push_file(
                os.path.join(s3_uri, os.path.basename(f)),
                f
            )
            files.append(os.path.join(s3_uri, os.path.basename(f)))
        shutil.rmtree(tmp_dir)
        return files

    def to_dict(self, entry):
        """
        Return a dictionnary from a stream entry
        Ex : (5, "http://www.site.com/", 200) will return {"id": 5, "url": "http://www.site.com/", "http_code": 200}
        """
        return {field[0]: value for field, value in izip(self.HEADERS, entry)}

    @classmethod
    def create_temporary_dataset(cls):
        return TemporaryDataset(
            stream_def=cls()
        )


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

    def persist(self, directory, first_part_id_size=FIRST_PART_ID_SIZE, part_id_size=PART_ID_SIZE, sort=True):
        if sort:
            self.sort()
        return self.stream_def.persist(stream=iter(self.dataset),
                                       directory=directory,
                                       first_part_id_size=first_part_id_size,
                                       part_id_size=part_id_size)

    def persist_to_s3(self, s3_uri, first_part_id_size=FIRST_PART_ID_SIZE, part_id_size=PART_ID_SIZE, sort=True):
        if sort:
            self.sort()
        return self.stream_def.persist_to_s3(stream=iter(self.dataset),
                                             s3_uri=s3_uri,
                                             first_part_id_size=first_part_id_size,
                                             part_id_size=part_id_size)
