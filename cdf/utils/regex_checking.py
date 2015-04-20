"""Check a regex against our reduced syntax

"""

from enum import Enum
import re

_specials = set(r"^.*+?\|[(){$")


class RegexError(Exception):
    """
    Exception on parsing a RE.

    `token` represents the error, `pos` is its position. `c` is mostly useless.
    """
    def __init__(self, token, c, pos):
        super(RegexError, self).__init__()
        self.token = token
        self.c = c
        self.pos = pos

    def __str__(self):
        return "Error {} at {}".format(self.token, self.pos)


class Token(Enum):
    Normal = 1,
    KnownEscape = 2,
    UnrecognizedEscapeError = 3,
    BackslashAtEOLError = 4,
    GroupStart = 5,
    BadGroupError = 6,
    GroupEnd = 7,
    Kleene = 8,
    Or = 9,
    Anchor = 10,
    Any = 11,
    QMark = 12,
    QMarkAlreadySeenError = 13,  # To guard against 'x*??', 'x???' ...
    Class = 14,
    BadClassError = 15,
    Quantifier = 16,
    BadQuantifierError = 17,
    BadKleeneError = 18,
    BadQMarkError = 19,
    BadGroupExtensionError = 20,


# Tokens which can precede '*', '+' '{}', '?'
_REPEATABLE_TOKENS = {Token.Normal, Token.Any, Token.KnownEscape, Token.Class, Token.Anchor, Token.GroupEnd}


class CharClass(object):
    """
    Character class: [a-zA-Z].
    """
    def __init__(self, reverse, content):
        self.reverse = reverse
        self.content = content


class ParserState(object):
    """
    State of the RE parser. Iterable, returning (Token, str).

    This is mostly a tokenizer, though it sometimes does (a bit too) more.
    """

    # \w etc; \A \z; \t \r \n
    # XXX python doesn't support z, only Z; re2 supports z and not Z; .Net accepts both.
    _KNOWN_ESCAPES = set("wsdWSD" + "Az" + "trn")
    # {m[,[n]]}
    _RE_QUANTIFIER = re.compile(r"(\d+)(?:,(\d*))?\}")
    # <name>
    _RE_NAMED_GROUP = re.compile(r"<(\w+)>")

    def __init__(self, regex):
        """
        Initialize the ParserState
        :param regex: The regex string
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
        """
        Tokenize c
        :param c: current character (at self.pos-1)
        :type c: str
        :return: (Token.Type, value)
        :rtype: tuple
        """
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
            return Token.GroupEnd, None
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
        if c == '{':
            return self._get_quantifier()

    def _get_escaped(self):
        """
        Check and return escaped character at self.pos
        :return:
        :rtype:
        """
        i = self.pos
        if i < self.len:
            self.pos += 1
            return self._classify_escape(self.regex[i])
        return Token.BackslashAtEOLError, '\\'

    def _classify_escape(self, c):
        """
        Classify the escape sequence: known or unrecognized.
        :param c:
        :type c: str
        :return: Token, c
        :rtype: tuple
        """
        if c in _specials or c in ']}':
            return Token.Normal, c
        if c in self._KNOWN_ESCAPES:
            return Token.KnownEscape, c
        return Token.UnrecognizedEscapeError, c
        # raise RegexError("Unsupported escape sequence")

    def _get_parens(self):
        """
        Check parens type (normal or extended), increment nparens.
        :return: (Token, type)
        :rtype:
        """
        i = self.pos
        if i >= self.len:
            return Token.BadGroupError, None
        if self.regex[i] == '?':
            # extended notation
            self.pos += 1
            i += 1
            extension = self.regex[i]
            self.pos += 1
            i += 1
            if i >= self.len:
                return Token.BadGroupError, None
            if extension == ':':
                pass
            elif extension == 'P':
                mo = self._RE_NAMED_GROUP.match(self.regex, i)
                if not mo:
                    return Token.BadGroupError, None
                self.pos = mo.end()
                extension = mo.group(1)
            else:
                return Token.BadGroupExtensionError, None
        else:
            extension = None
        self.nparens += 1
        return Token.GroupStart, extension

    def _get_char_class(self):
        """
        Parse a character class.
        To cope with []] and [^]], don't check ']' on first char. We also don't need to check '-' at start
        ('\' however must be recognized here)
        :return: (Token, CharClass) or (Token.BadClassError, None)
        :rtype: tuple
        """
        content = []
        reverse = False
        prev_char = None
        i = self.pos
        if i >= self.len:
            return Token.BadClassError, None
        # TODO? [[:xxx:]]
        if self.regex[i:i+2] == '[:':
            p = self.regex.find(':]], i+2')
            if p == -1:
                return Token.BadClassError, None
            return Token.BadClassError, None
        c = self.regex[i]
        # if i + 1 >= self.len:
        #     return Token.BadClassError, None
        if c == '^':
            reverse = True
            self.pos += 1
            i += 1
            # if i >= self.len:
            #     return Token.BadClassError, None
        start = True
        while 1:
            if i >= self.len:
                return Token.BadClassError, None
            c = self.regex[i]
            if c == ']':
                if not start:
                    break
                # no elif: we want to go to `content.append(c)`
            if c == '-' and prev_char is not None and self.regex[i + 1] != ']':
                i += 1
                # if i >= self.len:
                #     return Token.BadClassError, None
                c = self.regex[i]
                if c == '\\':
                    i += 1
                    # if i >= self.len:
                    #     return Token.BadClassError, None
                    c = self.regex[i]
                if c < prev_char:
                    return Token.BadClassError, None
                content.append('-')
                content.append(c)
                # [b-d-a] is b|c|d|-|a: the second '-' doesn't mark a range. So prev_char must be reset after 'd'.
                prev_char = None
            elif c == '\\':
                i += 1
                # if i >= self.len:
                #     return Token.BadClassError, None
                c = self.regex[i]
                content.append(c)
                prev_char = c
            else:
                content.append(c)
                prev_char = c
            i += 1
            start = False
        self.pos = i + 1
        return Token.Class, CharClass(reverse, ''.join(content))

    def _get_quantifier(self):
        """
        Parse {m[,[n]]}.
        Check that m <= n.
        :return: (Token, values). m => int; m, => (int, ); m,n => (int, int)
        :rtype: tuple
        """
        mo = self._RE_QUANTIFIER.match(self.regex, self.pos)
        if not mo:
            return Token.BadQuantifierError, None
        self.pos = mo.end()
        m, n = mo.groups()
        try:
            m = int(m, 10)
        except ValueError:
            return Token.BadQuantifierError, None
        if n is None:
            return Token.Quantifier, m
        if n != '':
            try:
                n = int(n, 10)
            except ValueError:
                return Token.BadQuantifierError, None
            if m > n:
                return Token.BadQuantifierError, None
            return Token.Quantifier, (m, n)
        return Token.Quantifier, (m, )


def check(regex):
    """
    Main entry point.
    :param regex: The regex to test
    :type regex:
    :return: True if OK, raise on error.
    :rtype:
    """
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
    parens_pos = []
    for tc in parser:
        if not tc:
            # Unknown error
            raise RegexError(None, None, parser.pos)
        token, c = tc
        ok = False
        if token == Token.GroupStart:
            # '(': push position on our stack
            parens_pos.append(parser.pos)
            ok = True
        elif token == Token.GroupEnd:
            # ')': group OK, forget it
            parens_pos.pop()
            ok = True
        elif token in _REPEATABLE_TOKENS or token == Token.Or:
            # The usual suspects
            ok = True
        elif token == Token.Kleene:
            # (Yep, "$*" is valid)
            ok = prev_token in _REPEATABLE_TOKENS
            if not ok:
                token = Token.BadKleeneError
        elif token == Token.QMark:
            # "?" has two distinct roles; as a quantifier ...
            ok = prev_token in _REPEATABLE_TOKENS
            if not ok:
                # ... or to modify a quantifier.
                ok = prev_token in (Token.Kleene, Token.QMark, Token.Quantifier)
                if ok:
                    # As a modifier, it cannot be applied multiple time; "*??" will be detected as an error
                    token = Token.QMarkAlreadySeenError
            if not ok:
                token = Token.BadQMarkError
        elif token == Token.Quantifier:
            # Just like "*" and "+", but we also check the values
            ok = prev_token in _REPEATABLE_TOKENS
            if ok:
                if not isinstance(c, int):
                    c = c[-1]  # last value
                ok = c < 1000  # re2's limit
            if not ok:
                token = Token.BadQuantifierError
        if ok:
            prev_token = token
            continue
        raise RegexError(token, c, parser.pos)
    if parser.nparens > 0:
        raise RegexError(Token.BadGroupError, '(', parens_pos[-1])
    return True
