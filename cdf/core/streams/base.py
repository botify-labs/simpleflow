from itertools import izip
import gzip

from cdf.core.streams.caster import Caster
from cdf.core.streams.utils import split_file


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

    def get_stream_from_path(self, path):
        cast = Caster(self.HEADERS).cast
        return StreamInstance(
            self,
            cast(split_file(gzip.open(path)))
        )

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
