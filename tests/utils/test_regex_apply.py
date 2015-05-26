import unittest

import re
from cdf.utils.regex_checking import apply_regex_rule


class TestApply(unittest.TestCase):
    def test_list_1(self):
        res = apply_regex_rule(
            '''<body><a class="home" href="/home.html">This is Home</a>
<a class="home" href="/home2.html">This is another Home</a>
</body>''', {

                'regex': r'<a class="home"[^>]*>(.*?)</a',
                'match': r'Homy: \1',
                'agg': 'list',
                'cast': 's',
                'ignore_case': True,
            }
        )
        self.assertEqual(['Homy: This is Home', 'Homy: This is another Home'], res)

    def test_exists(self):
        res = apply_regex_rule(
            '''<body><a class="home" href="/home.html">This is Home</a>
            <a class="home" href="/home2.html">This is another Home</a>
            </body>''', {
                'regex': r'<a class="home"[^>]*>(.*?)</a',
                'match': r'Homy: \1',
                'agg': 'exists',
                'cast': 'b',
                'ignore_case': True,
            }
        )
        self.assertTrue(res)

    def test_exists_not(self):
        res = apply_regex_rule(
            '''<body><a class="nohome" href="/home.html">This is Home</a>
            <a class="nohome" href="/home2.html">This is another Home</a>
            </body>''', {
                'regex': r'<a class="home"[^>]*>(.*?)</a',
            'match': r'Homy: \1',
            'agg': 'exists',
            'cast': 'b',
            'ignore_case': True,
        })
        self.assertFalse(res)

    def test_first(self):
        res = apply_regex_rule(
            '''<body><a class="home" href="/home.html">This is Home</a>
            <a class="home" href="/home2.html">This is another Home</a>
            </body>''', {
            'regex': r'<a class="home"[^>]*>(.*?)</a',
            'match': r'Homy: \1',
            'agg': 'first',
            'cast': 's',
            'ignore_case': True,
        })
        self.assertEqual('Homy: This is Home', res)

    def test_first_none(self):
        res = apply_regex_rule(
            '''<body><a class="nohome" href="/home.html">This is Home</a>
            <a class="nohome" href="/home2.html">This is another Home</a>
            </body>''', {
            'regex': r'<a class="home"[^>]*>(.*?)</a',
            'match': r'Homy: \1',
            'agg': 'first',
            'cast': 's',
            'ignore_case': True,
        })
        self.assertEqual(None, res)

    def test_count(self):
        res = apply_regex_rule(
            '''<body><a class="home" href="/home.html">This is Home</a>
            <a class="home" href="/home2.html">This is another Home</a>
            </body>''', {
            'regex': r'<a class="home"[^>]*>(.*?)</a',
            'match': r'Homy: \1',
            'agg': 'count',
            'cast': 's',
            'ignore_case': True,
        })
        self.assertEqual(2, res)

    def test_count_none(self):
        res = apply_regex_rule(
            '''<body><a class="nohome" href="/home.html">This is Home</a>
            <a class="nohome" href="/home2.html">This is another Home</a>
            </body>''', {
            'regex': r'<a class="home"[^>]*>(.*?)</a',
            'match': r'Homy: \1',
            'agg': 'count',
            'cast': 's',
            'ignore_case': True,
        })
        self.assertEqual(0, res)

    def test_cast_i(self):
        res = apply_regex_rule(
            '''<body><div>123</div><div>456</div></body>''', {
            'regex': r'<div>(.*?)</div',
            'match': r'\1',
            'agg': 'first',
            'cast': 'i',
            'ignore_case': True,
        })
        self.assertEqual(123, res)

    def test_cast_f(self):
        res = apply_regex_rule(
            '''<body><div>123</div><div>456</div></body>''', {
            'regex': r'<div>(.*?)</div',
            'match': r'\1',
            'agg': 'first',
            'cast': 'f',
            'ignore_case': True,
        })
        self.assertEqual(123.0, res)

    def test_cast_li(self):
        res = apply_regex_rule(
            '''<body><div>123</div><div>456</div></body>''', {
            'regex': r'<div>(.*?)</div',
            'match': r'\1',
            'agg': 'list',
            'cast': 'i',
            'ignore_case': True,
        })
        self.assertEqual([123, 456], res)

    def test_cast_lf(self):
        res = apply_regex_rule(
            '''<body><div>123</div><div>456</div></body>''', {
            'regex': r'<div>(.*?)</div',
            'match': r'\1',
            'agg': 'list',
            'cast': 'f',
            'ignore_case': True,
        })
        self.assertEqual([123.0, 456.0], res)

    def test_err(self):
        with self.assertRaises(Exception):
            res = apply_regex_rule(
                '<body><a class="home" href="/home.html">This is Home</a></body>', {
                    'regex': r'*',
                    'match': 'Homy: $1',
                    'agg': 'list',
                    'cast': 's',
                    'ignore_case': True,
                })
