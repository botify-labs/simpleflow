from itertools import izip
import os
import gzip

from cdf.core.streams.caster import Caster
from cdf.core.streams.utils import split_file
from cdf.utils.s3 import fetch_file


class StreamBase(object):

    @property
    def primary_key_idx(self):
        """
        Return the index of "id" field
        """
        for i, field in enumerate(self.HEADERS):
            if field[0] == 'id':
                return i
        raise Exception('Primary key not found')

    def field_idx(self, field):
        """
        Return the field position of field in HEADERS
        """
        return map(lambda i: i[0], self.HEADERS).index(field)

    def get_stream_from_path(self, path):
        cast = Caster(self.HEADERS).cast
        return StreamInstance(
            self,
            cast(split_file(gzip.open(path)))
        )

    def get_stream_from_storage(self, storage_uri, tmp_dir, part_id, force_fetch=False):
        path, fetched = fetch_file(
            os.path.join(storage_uri, '{}.txt.{}.gz'.format(self.FILE, part_id)),
            os.path.join(tmp_dir, '{}.txt.{}.gz'.format(self.FILE, part_id)),
            force_fetch=force_fetch
        )
        return self.get_stream_from_path(path)

    def to_dict(self, stream):
        return {field[0]: value for field, value in izip(self.HEADERS, stream)}


class StreamInstance(object):

    def __init__(self, stream_type, stream):
        """
        :param stream_type : A StreamBase instance
        :param stream : a python stream
        """
        self.stream_type = stream_type
        self.stream = stream
