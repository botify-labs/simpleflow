# Order of masks is important !
NOFOLLOW_MASKS = [
    (8, "config"),
    (4, "robots"),
    (2, "meta"),
    (1, "link"),
]


def follow_mask(val):
    """
    In raw files, an url can be at the same time as "robots_no_follow" and "meta_no_follow"
    We return a concatenated version
    """
    if val == '0':
        return ["follow"]
    masks = []
    _mask = int(val)
    for bitmask, term in NOFOLLOW_MASKS:
        if bitmask & _mask == bitmask:
            masks.append("nofollow_{}".format(term))
    return masks
