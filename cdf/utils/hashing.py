from cdf.utils.convert import to_int64, to_int32


def string_to_int64(string):
    import pyhash
    hasher_64 = pyhash.fnv1_64()
    return to_int64(hasher_64(string))


def string_to_int32(string):
    import pyhash
    hasher_32 = pyhash.fnv1_32()
    return to_int32(hasher_32(string))