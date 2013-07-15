import pyhash
hasher = pyhash.fnv1_64()

from cdf.utils.convert import to_int64


def string_to_int64(url):
    return to_int64(hasher(url))
