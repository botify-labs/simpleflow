import itertools


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
    for _, g in itertools.groupby(input_stream,
                                  lambda x: _get_part_index(x[0], nb_parts, stream_size)):
        #remove enumeration index
        yield itertools.imap(lambda x: x[1], g)
    #generate empty lists for remaining elements
    for _ in range(stream_size, nb_parts):
        yield []
