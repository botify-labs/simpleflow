# -*- coding: utf-8 -*-
import itertools
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
_PREV_NEXT_MASK_IDS = map(lambda x: x[1], _PREV_NEXT_MASKS)


def compute_nofollow_combination(keys, allowed_mask_ids):
    """Compute a canonical string for a list of nofollow keys.
    :param keys: the input list of nofollow keys
    :type keys: list
    :param allowed_mask_ids: the list of mask ids that can figure
                             in the nofollow combination
    :type allowed_mask_ids: list
    :returns: string
    """
    return '_'.join(
        sorted([k for k in keys if k in allowed_mask_ids])
    )

def build_nofollow_combination_lookup(mask_ids, allowed_mask_ids):
    """Given a list of mask ids build a lookup table tuple -> nofollow_combination.
    The dict keys are all the possible permutations of mask ids
    and nofollow combination is a canonical string that represents the combination.

    For instance if mask_ids is ["robots", "meta"] the dict will be
    () -> ""
    ("robots") -> "robots"
    ("meta") -> "meta"
    ("robots", "meta") -> "meta_robots"
    ("meta", "robots") -> "meta_robots"

    :param mask_ids: the input list of mask ids as a list of strings
    :type mask_ids: list
    :param allowed_mask_ids: the list of mask ids that can figure
                             in the nofollow combination
    :type allowed_mask_ids: list
    :returns: dict
    """
    result = {}
    for i in range(len(mask_ids) + 1):
        for keys in itertools.permutations(mask_ids, i):
            result[tuple(keys)] = compute_nofollow_combination(
                keys, allowed_mask_ids
            )
    return result

#a lookup table to get the nofollow combination given a list of keys
KEY_TO_NOFOLLOW_COMBINATION = build_nofollow_combination_lookup(
    _NOFOLLOW_MASK_IDS + _PREV_NEXT_MASK_IDS,
    _NOFOLLOW_MASK_IDS
)


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
