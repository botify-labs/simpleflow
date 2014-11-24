# -*- coding: utf-8 -*-
from enum import Enum

"""
Real bitmask is :

urlinfo_mask :
1  : gzip
2  : (not used)
4  : meta no index
8  : meta nofollow
16 : has canonical
32 : bad canonical
64 : blocked by config (! not possible = always 0)
"""


class UrlInfosMask(Enum):
    '''A class to represent the different flags in urlinfos mask'''
    GZIP = 1
    META_NOINDEX = 4
    META_NOFOLLOW = 8
    HAS_CANONICAL = 16
    BAD_CANONICAL = 32
    BLOCKED_BY_CONFIG = 64


def urlinfos_mask(mask):
    """Interpret the urlinfos mask to extract the encoded information
    :param mask: the input maks
    :type mask: int
    :returns: list
    """
    masks = []
    if mask < 0:
        return masks
    return [flag for flag in UrlInfosMask if flag.value & mask != 0]
