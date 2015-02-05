# TODO merge with below `is_link_external`
def is_link_internal(link_mask, dest, is_bitmask=False):
    """Determine if a link is an internal link

    A special case is handled here: if an internal link is blocked by
    robots.txt, our crawler will not allocate it an url id, so the
    destination url id will be -1. This kind of link is treated as an
    internal link.

    Accepts bitmask or decoded nofollow list.

    :param link_mask: the bitmask of the link
    :type link_mask: int, list
    :param dest: the url id of the link destination
    :type dest: int
    :param is_bitmask: flag indicates the type of `link_mask`
    :type is_bitmask: bool
    """
    is_robots = link_mask & 4 == 4 if is_bitmask else 'robots' in link_mask
    return dest > 0 or (dest == -1 and is_robots)


def is_external_link(link_mask):
    """Determine if a link is an external link
    All we need to do here is check the presence of bit `8`

    :param link_mask: link bitmask value
    :type link_mask: int
    :return:
    """
    return link_mask & 8 == 8


def is_link(link_type):
    """Determine if a link is a normal <a> link
    """
    return link_type == 'a'


def is_follow_link(link_mask, is_bitmask=False):
    """Determine if a link is a follow link or not
    :param link_mast: the bitmask of the link
    :type link_mask: int, list
    :param is_bitmask: flag indicates the type of `link_mask`
    :type is_bitmask: bool
    """
    if is_bitmask:
        link_mask = link_mask & 31
        return link_mask in (0, 8)
    else:
        return "follow" in link_mask
