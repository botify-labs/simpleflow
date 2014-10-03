from itertools import (
    takewhile,
    islice,
    count,
    groupby,
    imap
)

def split_list(input_list, nb_parts):
    return split_stream(input_list, len(input_list), nb_parts)


def _get_part_index(index, nb_parts, size):
    return (nb_parts * index)/size


def split_stream(input_stream, stream_size, nb_parts):
    """Split a stream into n chunks of approximatively the same size.
    :param input_stream: the stream to split
    :type input_stream: iterator
    :param stream_size: the size of the input stream.
    :type stream_size: int
    :param stream_size: int
    :param nb_parts: the number of chunks to generate.
    :type nb_parts: int
    :returns: iterator - a generator of chunks, each chunk being a generator.
    """
    if nb_parts <= 0:
        raise ValueError("nb_parts must be strictly positive")

    #add index to be able to compute the part index of each element.
    input_stream = enumerate(input_stream)
    for _, g in groupby(input_stream,
                                  lambda x: _get_part_index(x[0], nb_parts, stream_size)):
        #remove enumeration index
        yield imap(lambda x: x[1], g)
    #generate empty lists for remaining elements
    for _ in range(stream_size, nb_parts):
        yield []


def chunk(stream, size):
    """Chunk the input stream according to size

    >>> _chunk([1, 2, 3, 4, 5], 2)
    [[1, 2], [3, 4], [5]]

    This helper slice the input stream into chunk of `size`. At the end of
    the `stream`, `islice` will return an empty list [], which will stops
    the `takeWhile` wrapper
    """
    _stream = iter(stream)
    return takewhile(bool, (list(islice(_stream, size)) for _ in count()))
