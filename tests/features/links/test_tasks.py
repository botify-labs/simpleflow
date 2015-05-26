import tempfile
import json
from moto import mock_s3
import boto
import mock
import numpy as np

from cdf.features.main.streams import (
    IdStreamDef
)
from cdf.features.links.streams import (
    OutlinksRawStreamDef,
    OutlinksStreamDef,
    BadLinksStreamDef,
    BadLinksCountersStreamDef,
    OutlinksCountersStreamDef,
    OutcanonicalCountersStreamDef,
    OutredirectCountersStreamDef,
    LinksToNonCompliantStreamDef,
    LinksToNonCompliantCountersStreamDef,
    InlinksCountersStreamDef,
    InlinksPercentilesStreamDef,
    InredirectCountersStreamDef,
    PageRankStreamDef)
from cdf.features.main.streams import InfosStreamDef, CompliantUrlStreamDef
from cdf.features.links.tasks import (
    make_bad_link_file as compute_bad_link,
    make_links_counter_file as compute_link_counter,
    make_bad_link_counter_file as compute_bad_link_counter,
    make_top_domains_files as compute_top_domains,
    make_links_to_non_compliant_file,
    make_links_to_non_compliant_counter_file,
    make_inlinks_percentiles_file,
    page_rank
)

from cdf.features.main.reasons import encode_reason_mask, REASON_HTTP_CODE
from cdf.testing import TaskTestCase
from cdf.utils.s3 import list_files


# TODO(darkjh) remove duplication with stream-level test
class TestBadLinksTask(TaskTestCase):
    def setUp(self):
        self.info = [
            [1, 0, '', 1, 12345, 200, 1, 1, 1],
            [2, 0, '', 1, 12345, 301, 1, 1, 1],
            [3, 0, '', 1, 12345, 500, 1, 1, 1],
        ]
        self.outlinks = [
            [4, 'a', 0, 1, ""],
            [4, 'a', 0, 2, ""],
            [5, 'a', 0, 1, ""],
            [5, 'a', 0, 3, ""],
            [6, 'canonical', 0, 2, ""]
        ]
        self.tmp_dir = tempfile.mkdtemp()
        self.first_part_size = 2
        self.part_size = 1

    @mock_s3
    def test_harness(self):
        s3_uri = 's3://test_bucket'
        s3 = boto.connect_s3()
        s3.create_bucket('test_bucket')
        fake_crawl_id = 1234

        InfosStreamDef.persist(
            iter(self.info),
            s3_uri,
            first_part_size=self.first_part_size,
            part_size=self.part_size
        )

        OutlinksStreamDef.persist(
            iter(self.outlinks),
            s3_uri,
            first_part_size=self.first_part_size,
            part_size=self.part_size
        )

        compute_bad_link(
            fake_crawl_id, s3_uri,
            first_part_id_size=self.first_part_size,
            part_id_size=self.part_size
        )
        self.assertEqual(len(list_files(s3_uri, '.*badlinks.*')), 2)

        result = list(BadLinksStreamDef.load(s3_uri, tmp_dir=self.tmp_dir))
        expected = [
            [4, 2, 1, 301],
            [5, 3, 1, 500]
        ]
        self.assertEqual(result, expected)


class TestLinksCounterTask(TaskTestCase):
    def setUp(self):
        self.outlinks = [
            [1, 'a', 0, 2, ''],
            [1, 'a', 1, 3, ''],
            [1, 'a', 0, 4, ''],
            [3, 'canonical', 5, 5, ''],
            [4, 'r301', 5, 5, ''],
        ]
        self.tmp_dir = tempfile.mkdtemp()

    @mock_s3
    def test_outgoing_harness(self):
        s3 = boto.connect_s3()
        s3.create_bucket('test_bucket')
        s3_uri = 's3://test_bucket'
        fake_crawl_id = 1234

        stream_args = {
            'uri': s3_uri,
            'tmp_dir': self.tmp_dir,
            'part_id': 0
        }

        OutlinksStreamDef.persist(
            iter(self.outlinks), s3_uri
        )

        compute_link_counter(
            fake_crawl_id, s3_uri,
            part_id=0, link_direction='out'
        )

        # check links
        expected = [
            [1, ['follow'], True, 2, 2], [1, ['link'], True, 1, 1]
        ]
        result = list(OutlinksCountersStreamDef.load(**stream_args))
        self.assertItemsEqual(expected, result)

        # check canonical
        expected = [[3, False]]
        result = list(OutcanonicalCountersStreamDef.load(**stream_args))
        self.assertItemsEqual(expected, result)

        # check redirection
        expected = [[4, True]]
        result = list(OutredirectCountersStreamDef.load(**stream_args))
        self.assertItemsEqual(expected, result)


class TestBadLinkCounterTask(TaskTestCase):
    def setUp(self):
        self.badlinks = [
            [1, 2, 1, 500],
            [1, 9, 1, 500],
            [1, 2, 1, 400],
        ]
        self.tmp_dir = tempfile.mkdtemp()

    @mock_s3
    def test_harness(self):
        s3 = boto.connect_s3()
        s3.create_bucket('test_bucket')
        s3_uri = 's3://test_bucket'
        part_id = 64
        fake_crawl_id = 1234

        BadLinksStreamDef.persist(
            iter(self.badlinks),
            s3_uri,
            part_id=part_id
        )

        compute_bad_link_counter(
            fake_crawl_id, s3_uri,
            part_id=part_id
        )

        result = list(BadLinksCountersStreamDef.load(
            s3_uri,
            tmp_dir=self.tmp_dir,
            part_id=part_id
        ))
        expected = [
            [1, 400, 1],  # 1 400 link
            [1, 500, 2],  # 2 500 link
        ]
        self.assertEqual(result, expected)


class TestMakeLinksToNonCompliantFile(TaskTestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    @mock_s3
    def test_nominal_case(self):
        s3 = boto.connect_s3()
        bucket = s3.create_bucket('test_bucket')
        s3_uri = 's3://test_bucket'

        compliant_stream = iter([
            (1, True, encode_reason_mask()),
            (2, True, encode_reason_mask()),
            (3, False, encode_reason_mask(REASON_HTTP_CODE))
        ])
        CompliantUrlStreamDef.persist(
            compliant_stream,
            s3_uri
        )

        outlinks_stream = iter([
            (1, 'a', 0, 2, ""),
            (1, 'a', 0, 3, ""),
            (2, 'a', 0, 3, ""),
            (2, 'a', 4, 3, ""),
        ])
        OutlinksStreamDef.persist(
            outlinks_stream,
            s3_uri
        )

        first_part_id_size = 2
        part_id_size = 10
        make_links_to_non_compliant_file(
            s3_uri,
            first_part_id_size=first_part_id_size,
            part_id_size=part_id_size
        )

        actual_result = self.get_files(
            s3_uri, regexp='url_non_compliant_links.*')
        expected_result = [
            "s3://test_bucket/url_non_compliant_links.txt.0.gz",
            "s3://test_bucket/url_non_compliant_links.txt.1.gz"
        ]
        self.assertEqual(expected_result, actual_result)

        actual_stream = list(
            LinksToNonCompliantStreamDef.load(s3_uri, self.tmp_dir)
        )

        expected_stream = [
            [1, True, 3],
            [2, True, 3],
            [2, False, 3],
        ]
        self.assertEqual(expected_stream, list(actual_stream))


class TestMakeLinksToNonCompliantCounterFile(TaskTestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    @mock_s3
    def test_nominal_case(self):
        s3 = boto.connect_s3()
        bucket = s3.create_bucket('test_bucket')
        s3_uri = 's3://test_bucket'
        part_id = 3

        non_compliant_links_stream = iter([
            (1, 1, 3),
            (1, 1, 5),
            (3, 1, 1),
            (5, 1, 10),
            (5, 1, 11),
            (5, 1, 12)
        ])

        LinksToNonCompliantStreamDef.persist(
            non_compliant_links_stream,
            s3_uri,
            part_id=part_id
        )

        make_links_to_non_compliant_counter_file(
            s3_uri,
            part_id
        )

        actual_result = self.get_files(
            s3_uri, regexp='url_non_compliant_links_counters.*')
        expected_result = ["s3://test_bucket/url_non_compliant_links_counters.txt.3.gz"]
        self.assertEqual(expected_result, actual_result)

        actual_stream = LinksToNonCompliantCountersStreamDef.load(
            s3_uri,
            self.tmp_dir
        )
        expected_stream = [
            [1, 2, 2],
            [3, 1, 1],
            [5, 3, 3]
        ]

        self.assertEqual(expected_stream, list(actual_stream))


class TestMakeTopDomainsFiles(TaskTestCase):
    @mock_s3
    def test_nominal_case(self):
        #mock
        s3 = boto.connect_s3()
        bucket = s3.create_bucket('test_bucket')
        s3_uri = "s3://test_bucket"
        externals = iter([
            [0, "a", 8, -1, "http://foo.com/bar.html", ""],
            [0, "a", 8, -1, "http://bar.com/image.jpg", ""],
            [3, "a", 8, -1, "http://foo.com/qux.css", ""],
            [3, "canonical", 0, 0, ""],  # canonical
            [4, "a", 8, -1, "http://bar.foo.com/baz.html", ""],
            [4, "a", 8, -1, "http://bar.com/baz.html", ""],
            [4, "a", 0, 3, ""],  # internal link
            [4, "a", 8, -1, "http://foo.com/", ""],
            [4, "a", 8, -1, "foo", ""],  # invalid url
        ])
        OutlinksRawStreamDef.persist(externals, s3_uri)

        ids = iter([
            [0, "http", "host.com", "/url0", ""],
            [3, "http", "host.com", "/url3", ""],
            [4, "http", "host.com", "/url4", ""]
        ])
        IdStreamDef.persist(ids, s3_uri)

        #actual call
        crawl_id = 1001
        nb_top_domains = 10
        actual_result = compute_top_domains(
            crawl_id,
            s3_uri,
            nb_top_domains,
            crawled_partitions=[0]
        )

        #check file uris
        expected_result = [
            "s3://test_bucket/top_full_domains.json",
            "s3://test_bucket/top_second_level_domains.json"
        ]
        self.assertListEqual(expected_result, actual_result)

        #check file contents
        expected_top_domains = [
            {
                "domain": "foo.com",
                "follow_links": 3,
                "nofollow_links": 0,
                "unique_follow_links": 3,
                "unique_nofollow_links": 0,
                "follow_samples": [
                    {
                        "url": "http://foo.com/",
                        "unique_links": 1,
                        "sources": ["http://host.com/url4"]
                    },
                    {
                        "url": "http://foo.com/bar.html",
                        "unique_links": 1,
                        "sources": ["http://host.com/url0"]
                    },
                    {
                        "url": "http://foo.com/qux.css",
                        "unique_links": 1,
                        "sources": ["http://host.com/url3"]
                    }
                ],
                "nofollow_samples": []
            },
            {
                "domain": "bar.com",
                "follow_links": 2,
                "nofollow_links": 0,
                "unique_follow_links": 2,
                "unique_nofollow_links": 0,
                "follow_samples": [
                    {
                        "url": "http://bar.com/baz.html",
                        "unique_links": 1,
                        "sources": ["http://host.com/url4"]
                    },
                    {
                        "url": "http://bar.com/image.jpg",
                        "unique_links": 1,
                        "sources": ["http://host.com/url0"]
                    }
                ],
                "nofollow_samples": []
            },
            {
                "domain": "bar.foo.com",
                "follow_links": 1,
                "nofollow_links": 0,
                "unique_follow_links": 1,
                "unique_nofollow_links": 0,
                "follow_samples": [
                    {
                        "url": "http://bar.foo.com/baz.html",
                        "unique_links": 1,
                        "sources": ["http://host.com/url4"]
                    }
                ],
                "nofollow_samples": []
            }
        ]

        k = bucket.get_key("top_full_domains.json")
        actual_top_domains = json.loads(k.get_contents_as_string())
        self.assertEqual(expected_top_domains, actual_top_domains)

        expected_top_second_level_domains = [
            {
                "domain": "foo.com",
                "follow_links": 4,
                "nofollow_links": 0,
                "unique_follow_links": 4,
                "unique_nofollow_links": 0,
                "follow_samples": [
                    {
                        "url": "http://bar.foo.com/baz.html",
                        "unique_links": 1,
                        "sources": ["http://host.com/url4"]
                    },
                    {
                        "url": "http://foo.com/",
                        "unique_links": 1,
                        "sources": ["http://host.com/url4"]
                    },
                    {
                        "url": "http://foo.com/bar.html",
                        "unique_links": 1,
                        "sources": ["http://host.com/url0"]
                    },
                    {
                        "url": "http://foo.com/qux.css",
                        "unique_links": 1,
                        "sources": ["http://host.com/url3"]
                    }
                ],
                "nofollow_samples": []
            },
            {
                "domain": "bar.com",
                "follow_links": 2,
                "nofollow_links": 0,
                "unique_follow_links": 2,
                "unique_nofollow_links": 0,
                "follow_samples": [
                    {
                        "url": "http://bar.com/baz.html",
                        "unique_links": 1,
                        "sources": ["http://host.com/url4"]
                    },
                    {
                        "url": "http://bar.com/image.jpg",
                        "unique_links": 1,
                        "sources": ["http://host.com/url0"]
                    }
                ],
                "nofollow_samples": []
            }
        ]

        k = bucket.get_key("top_second_level_domains.json")
        actual_top_second_level_domains = json.loads(k.get_contents_as_string())
        self.assertEqual(
            expected_top_second_level_domains,
            actual_top_second_level_domains
        )


class TestMakeInlinksPercentileFile(TaskTestCase):
    def setUp(self):
        self.s3_uri = "s3://test_bucket"
        self.infos = [
            (1, 0, "", 0, 0, 200, 0, 0, 0, "?"),
            (2, 0, "", 1, 0, 200, 0, 0, 0, "?"),
            (3, 0, "", 1, 0, 200, 0, 0, 0, "?"),
            (4, 0, "", 1, 0, 200, 0, 0, 0, "?"),
            (5, 0, "", 1, 0, 200, 0, 0, 0, "?"),
            (6, 0, "", 1, 0, 200, 0, 0, 0, "?"),
        ]

        self.inlinks_count = [
            (1, 0, 10,  10),
            (2, 0, 2, 2),
            (3, 0, 1, 1),
            (4, 0, 6, 6),
            (6, 0, 8, 8)
        ]

        self.inredirections_count_stream = [
            (3, 4),
            (5, 2),
            (6, 1)
        ]

        self.tmp_dir = tempfile.mkdtemp()

    def _launch_task(self):
        s3 = boto.connect_s3()
        bucket = s3.create_bucket('test_bucket')

        urlids_stream = iter(self.infos)
        InfosStreamDef.persist(urlids_stream, self.s3_uri)

        first_part_size = 2
        part_size = 10
        InlinksCountersStreamDef.persist(
            self.inlinks_count,
            self.s3_uri,
            first_part_size=first_part_size,
            part_size=part_size
        )

        InredirectCountersStreamDef.persist(
            self.inredirections_count_stream,
            self.s3_uri,
            first_part_size=first_part_size,
            part_size=part_size
        )

        #mock files.json
        key = bucket.new_key("files.json")
        key.set_contents_from_string('{"max_uid_we_crawled": 6}')

        actual_result = make_inlinks_percentiles_file(
            self.s3_uri,
            first_part_size,
            part_size
        )
        return actual_result

    @mock_s3
    def test_inlinks_percentile_file(self):
        self._launch_task()
        actual_result = self.get_files(
            self.s3_uri, regexp='inlinks_percentiles.txt.*')

        expected_result = [
            "s3://test_bucket/inlinks_percentiles.txt.0.gz",
            "s3://test_bucket/inlinks_percentiles.txt.1.gz",
        ]
        self.assertEqual(expected_result, actual_result)

        actual_stream = InlinksPercentilesStreamDef.load(
            self.s3_uri,
            tmp_dir=self.tmp_dir
        )
        expected_stream = [
            [1, 6, 10],
            [2, 2, 2],
            [3, 3, 5],
            [4, 4, 6],
            [5, 1, 2],
            [6, 5, 9]
        ]
        self.assertEqual(expected_stream, list(actual_stream))

    @mock_s3
    def test_inlinks_percentile_graph_data(self):
        self._launch_task()
        s3 = boto.connect_s3()
        bucket = s3.get_bucket('test_bucket')

        key = bucket.get_key('precomputation/inlinks_percentiles.json')
        content = json.loads(key.get_contents_as_string())
        content = content['percentiles']

        self.assertEqual(len(content), 6)
        self.assertIn('avg', content[0])
        self.assertIn('min', content[0])
        self.assertIn('max', content[0])
        self.assertIn('metric_total', content[0])
        self.assertIn('url_total', content[0])


class TestPageRank(TaskTestCase):
    def setUp(self):
        self.s3_uri = "s3://test_bucket"
        self.outlinks = [
            [1, 'a', 0, 2, ''],
            [1, 'a', 0, 6, ''],
            [2, 'a', 0, 3, ''],
            [2, 'a', 0, 4, ''],
            [3, 'a', 0, 4, ''],
            [3, 'a', 0, 5, ''],
            [3, 'a', 0, 6, ''],
            [4, 'a', 0, 1, ''],
            [6, 'a', 0, 1, ''],
        ]

        self.infos = [
            [1, 0, '', 0, 0, 200, 0, 0, 0, 'fr'],
            [2, 0, '', 0, 0, 200, 0, 0, 0, 'fr'],
            [3, 0, '', 0, 0, 200, 0, 0, 0, 'fr'],
            [4, 0, '', 0, 0, 200, 0, 0, 0, 'fr'],
            [5, 0, '', 0, 0, 200, 0, 0, 0, 'fr'],
            [6, 0, '', 0, 0, 200, 0, 0, 0, 'fr'],
        ]
        self.tmp_dir = tempfile.mkdtemp()

    @mock_s3
    @mock.patch('cdf.features.links.tasks.get_crawl_info')
    def test_task(self, mock_crawl_info):
        mock_crawl_info.return_value = {'max_uid_we_crawled': 10}
        s3 = boto.connect_s3()
        bucket = s3.create_bucket('test_bucket')
        first_part_size = 2
        part_size = 10
        OutlinksRawStreamDef.persist(
            iter(self.outlinks), self.s3_uri,
            first_part_size=first_part_size,
            part_size=part_size
        )
        InfosStreamDef.persist(
            iter(self.infos), self.s3_uri,
            first_part_size=first_part_size,
            part_size=part_size
        )

        page_rank(
            self.s3_uri,
            first_part_id_size=first_part_size,
            part_id_size=part_size,
            tmp_dir=self.tmp_dir
        )

        result = self.get_files(
            self.s3_uri, regexp='pagerank.txt.*.gz')

        expected = [
            "s3://test_bucket/pagerank.txt.0.gz",
            "s3://test_bucket/pagerank.txt.1.gz",
        ]
        self.assertEqual(result, expected)

        result = list(PageRankStreamDef.load(
            self.s3_uri, tmp_dir=self.tmp_dir))

        rank_result = [l[1] for l in result]
        value_result = [l[2] for l in result]

        rank_expected = [1, 3, 5, 4, 6, 2]
        value_expected = np.array([
            0.3210154,
            0.1705440,
            0.1065908,
            0.1367922,
            0.0643121,
            0.2007454
        ])

        self.assertEqual(rank_result, rank_expected)
        self.assertTrue(
            np.linalg.norm(value_result - value_expected) < 0.001)
