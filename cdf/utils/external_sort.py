import itertools
import cPickle as pickle
import tempfile
import heapq
import os
import ujson as json
from abc import ABCMeta, abstractmethod

from cdf.log import logger


def split_iterable(iterable, block_size):
    """
    Splits an iterable into chunks of equal size.
    :param stream: the input stream
    :type stream: iterable
    :param block_size: the maximum number of elements in each generated file
    :type block_size: int
    """
    if block_size == 0:
        raise ValueError("block_size should not be null.")
    #add a prefix to be able to compute the chunk_id
    for chunk_id, chunk_elements in itertools.groupby(enumerate(iterable),
                                                      lambda x: x[0]/block_size):
        #remove prefix
        chunk_elements = itertools.imap(lambda x: x[1], chunk_elements)
        yield chunk_elements


def merge_sorted_streams(streams, custom_key):
    """Merge a sequence of sorted streams
    and generates a sorted stream out of them.
    :param streams: the input sequence of sorted streams
    :type streams: iterable
    :param custom_key: the key function that was used to sorted the input streams
                       if None, use the elements itself as a key
    :type custom_key: func
    :returns: generator
    """
    prefixed_streams = []
    for stream in streams:
        #create tuples (key, value) so that the heap uses the key to
        #sort the elements
        prefixed_stream = itertools.imap(lambda x: (custom_key(x), x), stream)
        prefixed_streams.append(prefixed_stream)
    for key, value in heapq.merge(*prefixed_streams):
        #only yield the value not the key
        yield value


class ExternalSort(object):
    """An interface for external sort algorithm implementation"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def external_sort(self, stream, key):
        """Sort an input stream given a custom criterion.
        The specificity of external sort is that it has low memory footprint
        (usually it dumps some of its data on temporary files)
        :param stream: the input stream
        :param stream: iterator
        :param key: the sort key
        :type key: function
        :returns: iterator
        """
        raise NotImplementedError()


class MergeExternalSort(ExternalSort):
    """A template class for merge external sort
    To sort a stream a merge external sort, splits the input stream into files
    of block_size elements,
    sort them one by one and generate a sorted stream from the sorted files.
    What needs to be specified for a concrete implementation
    of MergeExternalSort is:
        - how to serialize the stream elements in a file
        - how to read a stream of elements from a file
    """
    def __init__(self, block_size=100000):
        """Constructor.
        :param block_size: the number of elements contained
                           in each temporary file.
                           This parameter directly impacts
                           the number memory footprint of the algorithm.
                           However if the block size is too small there is a
                           risk to reach the maximum number of file handles.
        :type block_size: int
        """
        self.block_size = block_size

    def external_sort(self, stream, key):
        logger.debug("Splitting stream into chunks of size %d.", self.block_size)
        chunk_file_paths = []
        #split the stream into multiple chunks
        for chunk_elements in split_iterable(stream, self.block_size):
            #store each chunk in a pickle file
            chunk_file = tempfile.NamedTemporaryFile("wb", delete=False)
            for element in sorted(chunk_elements, key=key):
                self.dump(element, chunk_file)

            chunk_file.close()
            chunk_file_paths.append(chunk_file.name)

        #create one iterable from each chunk file
        chunk_streams = []
        for chunk_file in chunk_file_paths:
            chunk_streams.append(
                self.get_stream_from_file(open(chunk_file, "rb"))
            )

        logger.debug("Merging sorted stream chunks")
        for element in merge_sorted_streams(chunk_streams, key):
            yield element

        #delete chunk files
        for chunk_file_path in chunk_file_paths:
            os.remove(chunk_file_path)

    @abstractmethod
    def dump(self, element, f):
        raise NotImplementedError()

    @abstractmethod
    def get_stream_from_file(self, f):
        raise NotImplementedError()


class JsonExternalSort(MergeExternalSort):
    """Concrete implementation of MergeExternalSort.
    It uses json to serialize the objects.
    """
    def dump(self, element, f):
        f.write(json.dumps(element) + "\n")

    def get_stream_from_file(self, f):
        for row in f:
            yield json.loads(row)


class PickleExternalSort(MergeExternalSort):
    """Concrete implementation of MergeExternalSort
    It uses pickle to serialize the objects.
    """
    def dump(self, element, f):
        #use the highest protocol for better performance
        pickle.dump(element, f, pickle.HIGHEST_PROTOCOL)

    def get_stream_from_file(self, f):
        try:
            while True:
                yield pickle.load(f)
        except EOFError:
            pass
