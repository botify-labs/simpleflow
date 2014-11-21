# -*- coding: utf-8 -*-
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

# Order of masks is important !
_URLINFOS_MASKS = [
    (1, "gzip"),
    (4, "meta_noindex"),
    (8, "meta_nofollow"),
    (16, "has_canonical"),
    (32, "bad_canonical"),
    (64, "blocked_by_config")
]


def urlinfos_mask(mask):
    """Interpret the urlinfos mask to extract the encoded information
    :param mask: the input maks
    :type mask: int
    :returns: list
    """
    masks = []
    if mask < 0:
        return masks
    for bitmask, term in _URLINFOS_MASKS:
        if bitmask & mask != 0:
            masks.append(term)
    return masks
