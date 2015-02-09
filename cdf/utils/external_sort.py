import logging
import itertools
import cPickle as pickle
import tempfile
import heapq
import os
import marshal
import cbor

from abc import ABCMeta, abstractmethod

from cdf.compat import json

logger = logging.getLogger(__name__)


def external_sort(stream, key):
    """Sort a stream according to a key function.
    This is a commodity function that avoids
    the creation of an ExternalSort object.
    Use it when you need external sort but do not have specific requirements.
    The method will use a standard implementation of external sort
    :param stream: the input stream
    :type stream: iterator
    :param key: the sort key
    :type key: function
    :returns: iterator
    """
    #use MarshalExternalSort as it is the fastest method so far.
    external_sort = CborExternalSort()
    return external_sort.external_sort(stream, key)


class ExternalSort(object):
    """An interface for external sort algorithm implementation"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def external_sort(self, stream, key):
        """Sort an input stream given a custom criterion.
        The specificity of external sort is that it has low memory footprint
        (usually it dumps some of its data on temporary files)
        :param stream: the input stream
        :type stream: iterator
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
        #the maximum number of open files this instance will use.
        self.max_open_files_nb = 100

    def external_sort(self, stream, key):
        logger.debug("Splitting stream into chunks of size %d.", self.block_size)
        chunk_file_paths = []
        #a buffer to store data in memory
        #if the input stream is small, it will fit in the buffer
        #and no file creation will be required to sort the stream.
        _buffer = None
        #split the stream into multiple chunks
        for chunk_elements in split_iterable(stream, self.block_size):
            chunk_elements = sorted(list(chunk_elements), key=key)
            if _buffer is None:
                _buffer = chunk_elements
            else:
                #dump each chunk in a file
                chunk_file_path = self.dump_stream(_buffer)
                chunk_file_paths.append(chunk_file_path)
                _buffer = chunk_elements

        #merge files until there are no more than max_nb_files
        while len(chunk_file_paths) > self.max_open_files_nb:
            #-1 because the creation of the merged file will use a file handle
            files_to_merge = chunk_file_paths[:self.max_open_files_nb - 1]
            merged_file_path = self.merge_files(files_to_merge, key)

            chunk_file_paths = chunk_file_paths[self.max_open_files_nb - 1:]
            chunk_file_paths.append(merged_file_path)
            #remove merged files
            for path in files_to_merge:
                os.remove(path)

        logger.debug("Merging sorted stream chunks")
        streams = self.get_streams_from_files(chunk_file_paths)
        if _buffer is not None:
            streams.append(_buffer)
        for element in merge_sorted_streams(streams, key):
            yield element

        #delete chunk files
        for chunk_file_path in chunk_file_paths:
            os.remove(chunk_file_path)

    def dump_stream(self, stream):
        """Dumps a stream in a file.
        :param stream: the input stream
        :type stream: iterator
        :returns: str - the path to the file where the stream was dumped
        """
        dump_file = tempfile.NamedTemporaryFile("wb", delete=False)
        for element in stream:
                #we use the .file attribute because of
                #the Marshal implementation of MergeExternalSort.
                #indeed marshal.dump() requires a true file object to work
                #cf https://docs.python.org/2/library/marshal.html
                self.dump(element, dump_file.file)

        dump_file.close()
        return dump_file.name

    def get_streams_from_files(self, file_paths):
        """Return a list of streams from a list of file paths
        :param file_paths: the input list of file paths
        :type file_paths: list
        :rtype: list
        """
        streams = []
        for file_path in file_paths:
            streams.append(
                self.get_stream_from_file(open(file_path, "rb"))
            )
        return streams

    def get_stream_from_file_paths(self, file_paths, key):
        """Generates a sorted stream from a list of sorted files.
        :param file_paths: the path to the input files
        :type file_paths: list
        :param key: the sort key
        :type key: function
        :returns: iterator
        """
        streams = self.get_streams_from_files(file_paths)
        for element in merge_sorted_streams(streams, key):
            yield element

    def merge_files(self, file_paths, key):
        """Create a sorted file from a list of sorted files.
        :param file_paths: the path to the input files
        :type file_paths: list
        :param key: the sort key
        :type key: function
        :returns: str - the path to the generated file
        """
        result = self.dump_stream(
            self.get_stream_from_file_paths(file_paths, key)
        )
        return result

    @abstractmethod
    def dump(self, element, f):
        raise NotImplementedError()

    @abstractmethod
    def get_stream_from_file(self, f):
        #for the external sort to be memory efficient
        #it is required that this method can load the stream
        #from a file element by element
        #(by group of elements by group of elements if the group size is not
        #too big)
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


class MarshalExternalSort(MergeExternalSort):
    """Concrete implementation of MergeExternalSort.
    It uses marshal to serialize the objects.
    cf https://docs.python.org/2/library/marshal.html
    for more infos on marshal.
    """
    def dump(self, element, f):
        marshal.dump(element, f)

    def get_stream_from_file(self, f):
        while True:
            try:
                yield marshal.load(f)
            except EOFError:
                break


class CborExternalSort(MergeExternalSort):
    def dump(self, element, f):
        cbor.dump(element, f)

    def get_stream_from_file(self, f):
        while True:
            try:
                yield cbor.load(f)
            except (EOFError, IndexError):
                break


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


class BufferedExternalSort(ExternalSort):
    def __init__(self, sorter_class=MarshalExternalSort, buffer_size=100000):
        self.sorter_class = sorter_class
        self.buffer_size = buffer_size

    def take(self, stream):
        return list(itertools.islice(stream, self.buffer_size))

    def external_sort(self, stream, key):
        buffer = self.take(stream)
        try:
            elem = next(stream)
        except StopIteration:
            # stream exhausted, sort in memory
            return sorted(buffer, key=key)

        # stream larger than in-memory buffer
        # apply external sort
        sorter = self.sorter_class()
        buffer.append(elem)
        return sorter.external_sort(itertools.chain(buffer, stream), key=key)


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


