from cdf.core.streams.exceptions import GroupWithSkipException


def group_with(left, final_func=None, **stream_defs):
    """
    :param left: (stream, key_index, func)
    :param final_func : callable function executed at the end of a group
    :param **stream_defs: {stream_name: (stream, key_index, func)

    :returns:
    :rtype: yield (key, attributes)

    >>>> group_with(left=pattern_stream, infos=pattern_info)
    """
    id_ = {}
    right_line = {}
    left_stream, left_key_idx, left_func = left

    for line in left_stream:
        attributes = {}
        move_to_next_id = False
        current_id = line[left_key_idx]
        if left_func:
            left_func(attributes, line)

        for stream_name, stream_def in stream_defs.iteritems():
            if move_to_next_id:
                continue

            stream, key_idx, func = stream_def
            if not stream_name in id_:
                try:
                    right_line[stream_name] = stream.next()
                except StopIteration:
                    continue
                id_[stream_name] = right_line[stream_name][key_idx]

            while id_[stream_name] == current_id:
                """
                If one of the function raises a GroupWithException,
                the current id is ignored
                """
                if not move_to_next_id:
                    try:
                        func(attributes, right_line[stream_name])
                    except GroupWithSkipException:
                        move_to_next_id = True

                try:
                    right_line[stream_name] = stream.next()
                    id_[stream_name] = right_line[stream_name][key_idx]
                except StopIteration:
                    break

        if move_to_next_id:
            continue

        if final_func:
            try:
                final_func(attributes)
            except GroupWithSkipException:
                continue

        yield current_id, attributes
