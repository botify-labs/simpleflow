import unittest
import json
from cdf.features.sitemap.metadata import (Metadata,
                                           SitemapMetadata,
                                           SitemapIndexMetadata,
                                           Error)
from cdf.features.sitemap.document import SiteMapType


class TestMetadata(unittest.TestCase):
    def setUp(self):
        self.sitemap_index = "http://foo/sitemap_index.xml"

    def test_to_json(self):
        download_status = Metadata(
            [SitemapMetadata("http://foo/sitemap_1.xml",
                             "s3://foo/sitemap_1.xml",
                             self.sitemap_index)],
            [SitemapIndexMetadata("http://foo/sitemap_index.xml", 2, 1)],
            [Error("http://error1", SiteMapType.UNKNOWN, "foo", "bar"),
             Error("http://error2", SiteMapType.UNKNOWN, "foo", "bar")]
        )

        actual_result = download_status.to_json()

        expected_result = {
            "sitemaps": [
                {
                    "url": "http://foo/sitemap_1.xml",
                    "s3_uri": "s3://foo/sitemap_1.xml",
                    "sitemap_index": "http://foo/sitemap_index.xml"
                }
            ],
            "sitemap_indexes": [
                {
                    "url": "http://foo/sitemap_index.xml",
                    "valid_urls": 2,
                    "invalid_urls": 1
                }
            ],
            "errors": [
                {
                    "url": "http://error1",
                    "type": "UNKNOWN",
                    "error": "foo",
                    "message": "bar"
                },
                {
                    "url": "http://error2",
                    "type": "UNKNOWN",
                    "error": "foo",
                    "message": "bar"
                }
            ]
        }
        #compare the objects instead of the json representation
        #to be insensitive to item ordering
        self.assertEqual(expected_result["sitemap_indexes"], json.loads(actual_result)["sitemap_indexes"])
        self.assertEqual(expected_result, json.loads(actual_result))

    def test_to_json_no_sitemap(self):
        download_status = Metadata(
            [SitemapMetadata("http://foo/sitemap_1.xml",
                             "s3://foo/sitemap_1.xml",
                             None)]
        )

        actual_result = download_status.to_json()

        expected_result = {
            "sitemaps": [
                {
                    "url": u"http://foo/sitemap_1.xml",
                    "s3_uri": u"s3://foo/sitemap_1.xml",
                },
            ],
            "sitemap_indexes": [],
            "errors": []
        }
        #compare the objects instead of the json representation
        #to be insensitive to item ordering
        self.assertEqual(expected_result, json.loads(actual_result))

    def test_update(self):
        download_status = Metadata(
            [SitemapMetadata("http://foo/sitemap_1.xml",
                             "s3://foo/sitemap_1.xml",
                             self.sitemap_index)],
            [SitemapIndexMetadata("http://foo/sitemap_index_1.xml", 10, 0)],
            [Error("http://error1", SiteMapType.UNKNOWN, "DownloadError", ""),
             Error("http://error2", SiteMapType.UNKNOWN, "DownloadError", "")]
        )

        download_status_aux = Metadata(
            [SitemapMetadata("http://foo/sitemap_2.xml",
                             "s3://foo/sitemap_2.xml",
                             self.sitemap_index)],
            [SitemapIndexMetadata("http://foo/sitemap_index_2.xml", 2, 1)],
            [Error("http://error3", SiteMapType.UNKNOWN, "DownloadError", "")]
        )

        download_status.update(download_status_aux)
        expected_result = Metadata(
            [
                SitemapMetadata("http://foo/sitemap_1.xml",
                                "s3://foo/sitemap_1.xml",
                                self.sitemap_index),
                SitemapMetadata("http://foo/sitemap_2.xml",
                                "s3://foo/sitemap_2.xml",
                                self.sitemap_index)
            ],
            [
                SitemapIndexMetadata("http://foo/sitemap_index_1.xml", 10, 0),
                SitemapIndexMetadata("http://foo/sitemap_index_2.xml", 2, 1),
            ],
            [Error("http://error1", SiteMapType.UNKNOWN, "DownloadError", ""),
             Error("http://error2", SiteMapType.UNKNOWN, "DownloadError", ""),
             Error("http://error3", SiteMapType.UNKNOWN, "DownloadError", "")
            ]
        )
        self.assertEqual(expected_result, download_status)


