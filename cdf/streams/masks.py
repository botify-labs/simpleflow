# Order of masks is important !
FOLLOW_MASKS = [
    (0, "follow"),
    (8, "config_nofollow"),
    (4, "robots_nofollow"),
    (2, "meta_nofollow"),
    (1, "link_nofollow"),
]


def follow_mask(val):
    mask = int(val)
    for bitmask, term in FOLLOW_MASKS:
        if bitmask & mask == bitmask:
            return term


