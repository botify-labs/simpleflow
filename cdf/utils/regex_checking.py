__author__ = 'zeb'

class ParserState(object):
    def __init__(self, regex):
        self.regex = regex
        self.pos = -1


_specials = set(r"*+?\[](){}")

def check(regex):
    # Quick checks
    if not regex:
        return True
    specials = _specials
    for c in regex:
        if c in specials:
            break
    else:
        # Nothing special
        return True

    parser = ParserState(regex)

    # TODO we refuse everything here
    return False
