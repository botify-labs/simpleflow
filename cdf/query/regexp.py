

def normalize_regexp(regexp):
    """Normalize a python style regexp to Lucene's flavor

    It handles 2 cases for the moment:
      1. remove anchors in regexp (`^` and `$`)
      2.

    :param regexp: valid python regexp expression
    :type regexp: str
    :return: a Lucene valid regexp expression
    :rtype: str
    """
    s = regexp
    if s.startswith('^'):
        # drop starting anchor
        s = s[1:]
    else:
        s = '.*' + s
    if s.endswith('$'):
        # drop ending anchor
        s = s[:len(s) - 1]
    else:
        s += '.*'

    return s
