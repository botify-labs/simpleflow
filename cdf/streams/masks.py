# Order of masks is important !
FOLLOW_MASKS = [
    (8, "config_nofollow"),
    (4, "robots_nofollow"),
    (2, "meta_nofollow"),
    (1, "link_nofollow"),
]


def follow_mask(val):
    """
    In raw files, an url can be at the same time as "robots_no_follow" and "meta_no_follow"
    For the front report, we consider that the url has only 1 status. FOLLOW_MASKS constant follow the most to the less important
    """
    if val == '0':
        return "follow"
    _mask = int(val)
    for bitmask, term in FOLLOW_MASKS:
        if bitmask & _mask == bitmask:
            return term
