from operator import itemgetter
from itertools import izip, chain
import os
import gzip
import tempfile
import shutil

from cdf.core.streams.caster import Caster
from cdf.core.streams.utils import split_file
from cdf.utils import s3
from cdf.utils.path import get_files_ordered_by_part_id
from cdf.analysis.urls.utils import get_part_id

from boto.exception import S3ResponseError


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
    def get_stream_from_storage(cls, storage_uri, tmp_dir, part_id, force_fetch=False):
        """
        Return a Stream instance from a root storage uri.
        """
        try:
            path, fetched = s3.fetch_file(
                os.path.join(storage_uri, '{}.txt.{}.gz'.format(cls.FILE, part_id)),
                os.path.join(tmp_dir, '{}.txt.{}.gz'.format(cls.FILE, part_id)),
                force_fetch=force_fetch
            )
        except S3ResponseError:
            return cls.get_stream_from_iterator(iter([]))
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
        Return a stream from a iterable object
        """
        return Stream(cls(), i)

    def persist(self, stream, directory, part_id=None, first_part_id_size=1024, part_id_size=300000):
        if not os.path.isdir(directory):
            raise Exception('{} must be a directory'.format(directory))

        if part_id is not None:
            self._persist_part_id(stream, directory, part_id, first_part_id_size=1024, part_id_size=300000)

        f, current_part_id = None, None

        for entry in stream:
            local_part_id = get_part_id(entry[0], first_part_id_size, part_id_size)
            if local_part_id != current_part_id:
                if f:
                    f.close()
                current_part_id = local_part_id
                location = '{}.txt.{}.gz'.format(self.FILE, local_part_id)
                f = gzip.open(os.path.join(directory, location), 'w')
            f.write('\t'.join(str(k).encode('utf-8') for k in entry) + '\n')
        if f:
            f.close()

    def _persist_part_id(self, stream, directory, part_id, first_part_id_size=1024, part_id_size=300000):
        self.f = gzip.open(os.path.join(directory, '{}.txt.{}.gz'.format(self.FILE, part_id)), 'w')
        for entry in stream:
            self.f.write('\t'.join(str(k).encode('utf-8') for k in entry) + '\n')
        self.f.close()

    def persist_to_storage(self, stream, storage_uri, part_id=None, first_part_id_size=1024, part_id_size=300000):
        tmp_dir = tempfile.mkdtemp()
        self.persist(stream, directory=tmp_dir, part_id=part_id, first_part_id_size=first_part_id_size, part_id_size=part_id_size)
        for f in os.listdir(tmp_dir):
            s3.push_file(
                self.location,
                os.path.join(tmp_dir, f)
            )
        shutil.rmtree(tmp_dir)

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

    def __iter__(self):
        return self.iterator

    def next(self):
        return self.iterator.next()


class TemporaryDataset(object):
    """
    Create dataset files directly from a StreamDef object
    Warning : dataset is stored in a list, you need to ensure that the complete
    dataset can fit in memory
    """
    def __init__(self, stream_def):
        self.stream_def = stream_def
        self.dataset = []

    def append(self, *args):
        self.dataset.append(args)

    def sort(self, idx=0):
        self.dataset = sorted(self.dataset, key=itemgetter(0))

    def persist(self, directory, part_id=None, first_part_id_size=1024, part_id_size=300000, sort=True):
        if sort:
            self.sort()
        self.stream_def.persist(stream=iter(self.dataset), directory=directory, part_id=part_id, first_part_id_size=first_part_id_size, part_id_size=part_id_size)
