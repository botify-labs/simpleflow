# -*- coding: utf-8 -*-
"""
Real bitmask is :
1 link no follow
2 meta no follow
4 robots no follow
8 config no follow
16 extra link (if set & canonical -> this is NOT the¬
              first canonical in the page)¬
32 link with rel=prev
64 link with rel=next
"""

# Order of masks is important !
_NOFOLLOW_MASKS = [
    (4, "robots"),
    (2, "meta"),
    (1, "link"),
]
_NOFOLLOW_MASK_IDS = map(lambda x: x[1], _NOFOLLOW_MASKS)

_PREV_NEXT_MASKS = [
    (32, "next"),
    (64, "prev")
]


def follow_mask(mask):
    """Interpret the link mask to determine follow relation

    In raw files, an url can be at the same time as:
        "robots_no_follow" and "meta_no_follow"
    This function returns a concatenated version like:
        ["robots", "meta"]
    Nofollow list is always ordered according to following ordering:
        ["robots", "meta", "link"]
    """
    _mask = int(mask)
    if _mask & 7 == 0:
        masks = ["follow"]
    else:
        masks = [term for bitmask, term in _NOFOLLOW_MASKS if bitmask & _mask != 0]
    masks += [term for bitmask, term in _PREV_NEXT_MASKS if bitmask & _mask != 0]
    return masks


def prev_next_mask(mask):
    """Given a int mask returns a list containing the corresponding prev/next
    attributes.
    The result list contains strings.
    Its potential elements are "prev", "next"
    :param mask: the input mask
    :type mask: int
    :returns: list
    """
    flags = []
    for local_mask, name in _PREV_NEXT_MASKS:
        if int(mask) & local_mask == local_mask:
            flags.append(name)
    return flags


def list_to_mask(lst):
    mask = 0
    if lst == ['follow']:
        return 0
    for mask_int, mask_name in _NOFOLLOW_MASKS + _PREV_NEXT_MASKS:
        if mask_name in lst:
            mask += mask_int
    return mask


def is_first_canonical(mask):
    """Test if a canonical link is the first one of that page

        A dedicated bit is added in links file's mask:
            # 16 : extra link (if set & canonical -> this is NOT the
            first canonical in the page)
    """
    return int(mask) & 16 != 16
