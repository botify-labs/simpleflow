import unittest
import StringIO

from cdf.features.sitemap.document import (parse_sitemap_file,
                                           SiteMapType)


class TestParseSiteMapFile(unittest.TestCase):
    def test_sitemap_0_9(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        actual_result = parse_sitemap_file(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.SITEMAP, actual_result.type)
        self.assertEqual(["http://foo/bar"], actual_result.get_urls())

    def test_sitemap_index_0_9(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                        '<sitemap><loc>http://foo/sitemap.xml.gz</loc></sitemap>'
                        '</sitemapindex>')
        actual_result = parse_sitemap_file(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.SITEMAP_INDEX, actual_result.type)
        self.assertEqual(["http://foo/sitemap.xml.gz"], actual_result.get_urls())


    def test_sitemap_different_namespace(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.google.com/schemas/sitemap/0.84">'
                        '<url><loc>http://foo/bar</loc></url>'
                        '</urlset>')
        actual_result = parse_sitemap_file(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.SITEMAP, actual_result.type)
        self.assertEqual(["http://foo/bar"], actual_result.get_urls())

    def test_sitemap_multiple_namespaces(self):
        file_content = ('<?xml version="1.0" encoding="UTF-8"?>'
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:mobile="http://www.google.com/schemas/sitemap-mobile/1.0" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">'
                        '  <url>'
                        '<loc>http://foo/bar/baz</loc>'
                        '  </url>'
                        '</urlset>')
        actual_result = parse_sitemap_file(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.SITEMAP, actual_result.type)
        self.assertEqual(["http://foo/bar/baz"], actual_result.get_urls())

    def test_sitemap_no_namespace(self):
        file_content = ('<sitemapindex>'
                        '<sitemap><loc>http://foo.com/bar</loc></sitemap>'
                        '</sitemapindex>')
        actual_result = parse_sitemap_file(StringIO.StringIO(file_content))
        self.assertEqual(SiteMapType.SITEMAP_INDEX, actual_result.type)
        self.assertEqual(["http://foo.com/bar"], actual_result.get_urls())


