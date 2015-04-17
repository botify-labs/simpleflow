__author__ = 'zeb'

import unittest

# import re
from cdf.utils.regex_checking import *  # check, ParserState, Token


class BasicTests(unittest.TestCase):
    def test_empty(self):
        s = ""
        self.assertTrue(check(s))

    def test_simple(self):
        s = "toto"
        self.assertTrue(check(s))

    def test_parens(self):
        s = r"(abc)"
        self.assertTrue(check(s))

    def test_parens_open_1(self):
        s = r"abc("
        with self.assertRaises(RegexError):
            check(s)

    def test_parens_open_2(self):
        s = r"abc(def"
        with self.assertRaises(RegexError):
            check(s)

    def test_kleene_start(self):
        s = r"*"
        with self.assertRaises(RegexError):
            check(s)

    def test_kleene_double(self):
        s = r"*+"
        with self.assertRaises(RegexError):
            check(s)

    def test_kleene_double_2(self):
        s = r".**"
        with self.assertRaises(RegexError):
            check(s)

    def test_kleene_or(self):
        s = r"|+"
        with self.assertRaises(RegexError):
            check(s)

    def test_kleene_ok1(self):
        s = r"a+b*"
        self.assertTrue(check(s))

    def test_kleene_ok2(self):
        s = r"\d+\D*"
        self.assertTrue(check(s))

    def test_kleene_ok3(self):
        s = r"(xy)+(a|b)*"
        self.assertTrue(check(s))

    def test_kleene_ok4(self):
        s = r".+.*"
        self.assertTrue(check(s))

    def test_or_1(self):
        s = r"a|bc"
        self.assertTrue(check(s))

    def test_or_2(self):
        s = r"|"
        self.assertTrue(check(s))

    def test_q_1(self):
        s = r"?"
        with self.assertRaises(RegexError):
            check(s)

    def test_q_2(self):
        s = r"??"
        with self.assertRaises(RegexError):
            check(s)

    def test_q_3(self):
        s = r".?ab?"
        self.assertTrue(check(s))

    def test_q_4(self):
        s = r".*?ab+?"
        self.assertTrue(check(s))

    def test_q_5(self):
        s = r".??"
        self.assertTrue(check(s))

    def test_q_6(self):
        s = r".???"
        with self.assertRaises(RegexError):
            check(s)

    def test_q_7(self):
        s = r".+??"
        with self.assertRaises(RegexError):
            check(s)

    def test_q_6_p(self):
        s = r"(.???)"
        with self.assertRaises(RegexError):
            check(s)

    def test_q_7_p(self):
        s = r"(.+??)"
        with self.assertRaises(RegexError):
            check(s)

    def test_brace_open(self):
        s = r"["
        with self.assertRaises(RegexError):
            check(s)

    def test_brace_open_p(self):
        s = r"([)"
        with self.assertRaises(RegexError):
            check(s)

    def test_brace_open_not(self):
        s = r"[^"
        with self.assertRaises(RegexError):
            check(s)

    def test_class_empty(self):
        s = r"[]"
        with self.assertRaises(RegexError):
            check(s)

    def test_class_empty_p(self):
        s = r"([])"
        with self.assertRaises(RegexError):
            check(s)

    def test_class_empty_not(self):
        s = r"[^]"
        with self.assertRaises(RegexError):
            check(s)

    def test_class_bracket_open(self):
        s = r"[[]"
        self.assertTrue(check(s))

    def test_class_bracket_closed(self):
        s = r"[]]"
        self.assertTrue(check(s))

    def test_class_bracket_open_ab(self):
        s = r"[[ab]"
        self.assertTrue(check(s))

    def test_class_bracket_closed_ab(self):
        s = r"[]ab]"
        self.assertTrue(check(s))

    def test_class_bracket_not_open(self):
        s = r"[^[]"
        self.assertTrue(check(s))

    def test_class_bracket_not_closed(self):
        s = r"[^]]"
        self.assertTrue(check(s))

    def test_class_bracket_not_open_ab(self):
        s = r"[^[ab]"
        self.assertTrue(check(s))

    def test_class_bracket_not_closed_ab(self):
        s = r"[^]ab]"
        self.assertTrue(check(s))

    def test_class_bracket_a(self):
        s = r"[a]"
        self.assertTrue(check(s))

    def test_class_bracket_abc(self):
        s = r"[abc]"
        self.assertTrue(check(s))

    def test_class_bracket_cba(self):
        s = r"[cba]"
        self.assertTrue(check(s))

    def test_class_bracket_ac(self):
        s = r"[a-c]"
        self.assertTrue(check(s))

    def test_class_bracket_ac2(self):
        s = r"[ac-]"
        self.assertTrue(check(s))

    def test_class_bracket_ca(self):
        s = r"[c-a]"
        with self.assertRaises(RegexError):
            check(s)

    def test_class_bracket_ca2(self):
        s = r"[c\-a]"
        self.assertTrue(check(s))

    def test_class_bracket_bs_1(self):
        s = r"[abc\]]"
        self.assertTrue(check(s))

    def test_class_bracket_bs_2(self):
        s = r"[abc\]"
        with self.assertRaises(RegexError):
            check(s)

    def test_class_bracket_acAC(self):
        s = r"[a-cA-C]"
        self.assertTrue(check(s))

    def test_class_bracket_not_abc(self):
        s = r"[^abc]"
        self.assertTrue(check(s))

    def test_class_alpha(self):
        s = r"[[:alpha:]]"
        with self.assertRaises(RegexError):
            check(s)

    def test_class_not_upper(self):
        s = r"[[:^upper:]]"
        with self.assertRaises(RegexError):
            check(s)

    def test_quantifier_1(self):
        s = r"a{1}"
        self.assertTrue(check(s))

    def test_quantifier_2(self):
        s = r"a{1,}"
        self.assertTrue(check(s))

    def test_quantifier_3(self):
        s = r"a{1,2}"
        self.assertTrue(check(s))

    def test_quantifier_3_q(self):
        s = r"(a){1,2}"
        self.assertTrue(check(s))

    def test_quantifier_4(self):
        s = r"a{1,2}?"
        self.assertTrue(check(s))

    def test_quantifier_5(self):
        s = r"{1,2}"
        with self.assertRaises(RegexError):
            check(s)

    def test_quantifier_6(self):
        s = r"*{1,2}"
        with self.assertRaises(RegexError):
            check(s)

    def test_quantifier_7(self):
        s = r"a{1,2}*"
        with self.assertRaises(RegexError):
            check(s)

    def test_quantifier_8(self):
        s = r"a{1,2}??"
        with self.assertRaises(RegexError):
            check(s)

    def test_non_capturing(self):
        s = r"(?:...)"
        self.assertTrue(check(s))

    def test_named(self):
        s = r"(?P<name>...)"
        self.assertTrue(check(s))

    def test_bad_ext_1(self):
        s = r"(?P<name...)"
        with self.assertRaises(RegexError):
            check(s)

    def test_bad_ext_2(self):
        s = r"(?=...)"
        with self.assertRaises(RegexError):
            check(s)

    def test_bad_ext_3(self):
        s = r"(?"
        with self.assertRaises(RegexError):
            check(s)

    def test_bad_ext_4(self):
        s = r"(?)"
        with self.assertRaises(RegexError):
            check(s)

    def test_bad_ext_5(self):
        s = r"(?P"
        with self.assertRaises(RegexError):
            check(s)

    def test_bad_ext_6(self):
        s = r"(?P="
        with self.assertRaises(RegexError):
            check(s)

    def test_bad_ext_7(self):
        s = r"(?P<"
        with self.assertRaises(RegexError):
            check(s)

    def test_bad_ext_8(self):
        s = r"(?P<a"
        with self.assertRaises(RegexError):
            check(s)

    def test_bad_ext_9(self):
        s = r"(?P<a)"
        with self.assertRaises(RegexError):
            check(s)

    def test_bad_ext_10(self):
        s = r"(?P<>"
        with self.assertRaises(RegexError):
            check(s)

    def test_bad_ext_11(self):
        s = r"(?P<>)"
        with self.assertRaises(RegexError):
            check(s)


class ParserStateTests(unittest.TestCase):
    def test_empty(self):
        ps = ParserState("")
        with self.assertRaises(StopIteration):
            ps.next()

    def test_simple(self):
        ps = ParserState("xy")
        x = ps.next()
        self.assertEqual((Token.Normal, 'x'), x)
        x = ps.next()
        self.assertEqual((Token.Normal, 'y'), x)
        with self.assertRaises(StopIteration):
            ps.next()

    def test_escape_special(self):
        s = r"*+?\[](){}.^$|"
        s_escaped = '\\' + '\\'.join(s)
        ps = ParserState(s_escaped)
        # zz = list(zip(ps, s))
        for rx, x in zip(ps, s):
            self.assertEqual((Token.Normal, x), rx)

    def test_escape_eol(self):
        ps = ParserState("\\")
        x = ps.next()
        self.assertEqual((Token.BackslashAtEOLError, '\\'), x)
        with self.assertRaises(StopIteration):
            ps.next()

    def test_known_escape(self):
        s = "wdsWDSrnt"
        s_escaped = '\\' + '\\'.join(s)
        ps = ParserState(s_escaped)
        # zz = list(zip(ps, s))
        for rx, x in zip(ps, s):
            self.assertEqual((Token.KnownEscape, x), rx)

    def test_unknown_escape(self):
        s = "xGpP%0123456789"
        s_escaped = '\\' + '\\'.join(s)
        ps = ParserState(s_escaped)
        # zz = list(zip(ps, s))
        for rx, x in zip(ps, s):
            self.assertEqual((Token.UnrecognizedEscapeError, x), rx)

    def test_empty_parens(self):
        ps = ParserState("()")
        tok, c = ps.next()
        self.assertEqual(Token.GroupStart, tok)
        tok, c = ps.next()
        self.assertEqual(Token.GroupEnd, tok)
        with self.assertRaises(StopIteration):
            ps.next()

    def test_left_parens(self):
        ps = ParserState("(")
        for rx in ps:
            self.assertEqual((Token.BadGroupError, None), rx)

    def test_right_parens(self):
        ps = ParserState(")")
        for rx in ps:
            self.assertEqual((Token.BadGroupError, None), rx)

    def test_right_parens_2(self):
        ps = ParserState("xxx)x")
        for tok, c in ps:
            if tok == Token.Normal:
                self.assertEqual('x', c)
            else:
                self.assertEqual(Token.BadGroupError, tok)

    def test_parens_nc(self):
        ps = ParserState(r"(?:a)")
        tok, e = ps.next()
        self.assertEqual(Token.GroupStart, tok)
        self.assertEqual(':', e)
        rx = ps.next()
        self.assertEqual((Token.Normal, 'a'), rx)
        tok, e = ps.next()
        self.assertEqual(Token.GroupEnd, tok)

    def test_parens_named(self):
        ps = ParserState(r"(?P<toto>a)")
        tok, e = ps.next()
        self.assertEqual(Token.GroupStart, tok)
        self.assertEqual('toto', e)
        rx = ps.next()
        self.assertEqual((Token.Normal, 'a'), rx)
        tok, e = ps.next()
        self.assertEqual(Token.GroupEnd, tok)

    def test_star(self):
        ps = ParserState("*")
        tok, c = ps.next()
        self.assertEqual(Token.Kleene, tok)
        self.assertEqual('*', c)

    def test_plus(self):
        ps = ParserState("+")
        tok, c = ps.next()
        self.assertEqual(Token.Kleene, tok)
        self.assertEqual('+', c)

    def test_bracket_open(self):
        s = r"["
        ps = ParserState(s)
        tok, c = ps.next()
        self.assertEqual(Token.BadClassError, tok)

    def test_bracket_closed(self):
        s = r"]"
        ps = ParserState(s)
        tok, c = ps.next()
        self.assertEqual(Token.Normal, tok)
        self.assertEqual(']', c)

    def test_class_bracket_open(self):
        s = r"[[]"
        ps = ParserState(s)
        tok, c = ps.next()
        self.assertEqual(Token.Class, tok)
        self.assertIsInstance(c, CharClass)
        self.assertFalse(c.reverse)
        self.assertEqual("[", c.content)
        with self.assertRaises(StopIteration):
            ps.next()

    def test_class_bracket_closed(self):
        s = r"[]]"
        ps = ParserState(s)
        tok, c = ps.next()
        self.assertEqual(Token.Class, tok)
        self.assertIsInstance(c, CharClass)
        self.assertFalse(c.reverse)
        self.assertEqual("]", c.content)
        with self.assertRaises(StopIteration):
            ps.next()

    def test_class_bracket_bs_1(self):
        s = r"[abc\]]"
        ps = ParserState(s)
        tok, c = ps.next()
        self.assertEqual(Token.Class, tok)
        self.assertIsInstance(c, CharClass)
        self.assertFalse(c.reverse)
        self.assertEqual("abc]", c.content)
        with self.assertRaises(StopIteration):
            ps.next()

    def test_quantifier_1(self):
        s = r"{1}"
        ps = ParserState(s)
        tok, c = ps.next()
        self.assertEqual(Token.Quantifier, tok)
        self.assertIsInstance(c, int)
        self.assertEqual(1, c)
        with self.assertRaises(StopIteration):
            ps.next()

    def test_quantifier_2(self):
        s = r"{1,}"
        ps = ParserState(s)
        tok, c = ps.next()
        self.assertEqual(Token.Quantifier, tok)
        self.assertIsInstance(c, tuple)
        self.assertEqual((1,), c)
        with self.assertRaises(StopIteration):
            ps.next()

    def test_quantifier_3(self):
        s = r"{1,2}"
        ps = ParserState(s)
        tok, c = ps.next()
        self.assertEqual(Token.Quantifier, tok)
        self.assertIsInstance(c, tuple)
        self.assertEqual((1,2), c)
        with self.assertRaises(StopIteration):
            ps.next()
