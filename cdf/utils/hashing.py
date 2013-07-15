import pyhash
hasher = pyhash.fnv1_64()

from cdf.utils.num import to_u64


def string_to_u64(url):
    return to_u64(hasher(url))
