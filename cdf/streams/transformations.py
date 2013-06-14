def group_with(left, **stream_defs):
    """
    :param left: (stream, key_index, func)
    :param **stream_defs: {stream_name: (stream, key_index, func)

    :returns:
    :rtype: yield (key, attributes)

    >>>> group(patterns=pattern_stream, infos=pattern_info)
    """
    id_ = {}
    right_line = {}
    left_stream, left_key_idx, left_func = left

    for line in left_stream:
        attributes = {}
        current_id = line[left_key_idx]
        if left_func:
            left_func(attributes, line)

        for stream, key_idx, func in stream_defs.itervalues():
            if not stream in id_:
                right_line[stream] = stream.next()
                id_[stream] = right_line[stream][key_idx]

            while id_[stream] == current_id:
                func(attributes, right_line[stream])

                try:
                    right_line[stream] = stream.next()
                    id_[stream] = right_line[stream][key_idx]
                except StopIteration:
                    break
        yield current_id, attributes
