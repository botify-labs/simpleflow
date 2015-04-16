__author__ = 'zeb'

from enum import Enum
# import re

# Note: doesn't contain '.'
_specials = set(r"^.*+?\|[(){$")


class RegexError(Exception):
    """
    Exception on parsing a RE.
    """
    def __init__(self, token, c, pos):
        super(RegexError, self).__init__()
        self.token = token
        self.c = c
        self.pos = pos


class Token(Enum):
    Normal = 1,
    KnownEscape = 2,
    UnrecognizedEscapeError = 3,
    BackslashAtEOLError = 4,
    Group = 5,
    BadGroupError = 6,
    EndGroup = 7,  # Internal
    Kleene = 8,
    Or = 9,
    Anchor = 10,
    Any = 11,
    QMark = 12,
    QMarkThatsEnough = 13,  # Internal. To address 'x*??', 'x???' ...
    Class = 14,
    BadClassError = 15,


TOKEN_GROUP_ = {Token.Normal, Token.Any, Token.KnownEscape, Token.Group, Token.Class}


class ParserState(object):
    """
    State of the RE parser. Iterable, returning (Token, str).
    """

    # XXX python doesn't support z, only Z; re2 supports z and not Z; .Net accepts both.
    KNOWN_ESCAPES = set("wsdWSD" + "Az" + "trn")

    def __init__(self, regex):
        """
        Initialize the ParserState
        :param regex: The regex
        :type regex: str
        """
        self.pos = 0
        self.len = len(regex)
        self.regex = regex + ' '
        self.nparens = 0

    def __iter__(self):
        return self

    def next(self):
        """
        Iterator.
        :return:Token, str
        :rtype: tuple
        """
        i = self.pos
        if i < self.len:
            self.pos += 1
            return self._tokenize(self.regex[i])
        raise StopIteration()

    def _tokenize(self, c):
        if c not in _specials:
            return Token.Normal, c
        if c == '.':
            return Token.Any, c
        if c == '\\':
            return self._get_escaped()
        if c == '(':
            return self._get_parens()
        if c == ')':
            self.nparens -= 1
            if self.nparens < 0:
                return Token.BadGroupError, None
            return Token.EndGroup, None
        if c in '*+':
            return Token.Kleene, c
        if c == '?':
            return Token.QMark, c
        if c in '^$':
            return Token.Anchor, c
        if c == '|':
            return Token.Or, c
        if c == '[':
            return self._get_char_class()

    def _get_escaped(self):
        i = self.pos
        if i < self.len:
            self.pos += 1
            return self._classify_escape(self.regex[i])
        return Token.BackslashAtEOLError, '\\'
        # raise RegexError("\\ at end of expression")

    def _classify_escape(self, c):
        """
        Classify the escape.
        :param c:
        :type c: str
        :return: Token, c
        :rtype: tuple
        """
        if c in _specials or c in ']}':
            return Token.Normal, c
        if c in self.KNOWN_ESCAPES:
            return Token.KnownEscape, c
        return Token.UnrecognizedEscapeError, c
        # raise RegexError("Unsupported escape sequence")

    def _get_parens(self):
        content = []
        i = self.pos
        if i >= self.len:
            return Token.BadGroupError, None
        self.nparens += 1
        for token, c in self:
            if token == Token.BadGroupError:
                return token, c
            if token == Token.EndGroup:
                return Token.Group, content
            content.append((token, c))
        return Token.BadGroupError, content

    def _get_char_class(self):
        content = []
        reverse = False
        i = self.pos
        if i >= self.len:
            return Token.BadClassError, None
        c1 = self.regex[i]
        if i + 1 >= self.len:
            return Token.BadClassError, None
        # FIXME [[:xxx:]]
        if c1 == '^':
            reverse = True
            self.pos += 1
            i += 1
            if i >= self.len:
                return Token.BadClassError, None
            c1 = self.regex[i]
        while 1:
            content.append(c1)
            i += 1
            if i >= self.len:
                return Token.BadClassError, None
            c1 = self.regex[i]
            if c1 == ']':
                break


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
    prev_token = None
    for tc in parser:
        if not tc:
            raise RegexError(None, None, parser.pos)
        token, c = tc
        ok = False
        if token in TOKEN_GROUP_ or token in (Token.Anchor, Token.Or):
            ok = True
        elif token == Token.Kleene:
            ok = prev_token in TOKEN_GROUP_ or prev_token == Token.Anchor
        elif token == Token.QMark:
            ok = prev_token in TOKEN_GROUP_
            if not ok:
                ok = prev_token in (Token.Anchor, Token.Kleene, Token.QMark)
                if ok:
                    token = Token.QMarkThatsEnough
        if ok:
            prev_token = token
            continue
        raise RegexError(token, c, parser.pos)

    return True
