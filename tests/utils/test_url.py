import unittest
from cdf.utils.url import get_domain, get_second_level_domain


class TestGetDomain(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual("foo.com", get_domain("http://foo.com/bar.html"))

    def test_subdomain_case(self):
        self.assertEqual("foo.bar.com",
                         get_domain("http://foo.bar.com/baz.html"))


class TestGetSecondLevelDomain(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual(
            "foo.com",
            get_second_level_domain("http://foo.com/bar.html")
        )

    def test_subdomain_case(self):
        self.assertEqual(
            "baz.com",
            get_second_level_domain("http://foo.bar.baz.com/baz.html")
        )

    def test_composite_extension(self):
        self.assertEqual(
            "bar.co.uk",
            get_second_level_domain("http://foo.bar.co.uk/baz.html")
        )
