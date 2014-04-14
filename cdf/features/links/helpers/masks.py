# Order of masks is important !
_NOFOLLOW_MASKS = [
    (4, "robots"),
    (2, "meta"),
    (1, "link"),
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
    if _mask in (0, 8):
        return ["follow"]
    masks = []
    for bitmask, term in _NOFOLLOW_MASKS:
        if bitmask & _mask == bitmask:
            masks.append(term)
    return masks


def list_to_mask(lst):
    mask = 0
    if lst == ['follow']:
        return 0
    for mask_int, mask_name in _NOFOLLOW_MASKS:
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
