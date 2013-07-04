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

        for stream_name, stream_def in stream_defs.iteritems():
            stream, key_idx, func = stream_def
            if not stream_name in id_:
                try:
                    right_line[stream_name] = stream.next()
                except StopIteration:
                    continue
                id_[stream_name] = right_line[stream_name][key_idx]

            while id_[stream_name] == current_id:
                func(attributes, right_line[stream_name])

                try:
                    right_line[stream_name] = stream.next()
                    id_[stream_name] = right_line[stream_name][key_idx]
                except StopIteration:
                    break
        yield current_id, attributes
