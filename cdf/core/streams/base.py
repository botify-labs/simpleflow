from itertools import izip, chain
import os
import gzip

from cdf.core.streams.caster import Caster
from cdf.core.streams.utils import split_file
from cdf.utils import s3
from cdf.utils.path import get_files_ordered_by_part_id

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
        if part_id:
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

    def persist(self, stream, path, part_id=None, first_part_id_size=1024, part_id_size=300000):
        pass

    def to_dict(self, entry):
        """
        Return a dictionnary from a stream entry
        Ex : (5, "http://www.site.com/", 200) will return {"id": 5, "url": "http://www.site.com/", "http_code": 200}
        """
        return {field[0]: value for field, value in izip(self.HEADERS, entry)}


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
