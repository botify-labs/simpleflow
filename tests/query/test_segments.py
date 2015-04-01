import unittest
from StringIO import StringIO

from cdf.query.segments import (
    load_segments_idx_from_file,
    get_segments_from_args
)


class TestSegments(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_segments_from_args(self):
        results = [
            {"key": [111], "metrics": [10]},
            {"key": [222], "metrics": [8]}
        ]
        segments_idx = {
            111: {
                "human": "host=\"www.site.com\"",
                "query": {"field": "host", "value": "www.site.com"},
                "total_urls": 200
            },
            222: {
                "human": "host=\"www.site2.com\"",
                "query": {"field": "host", "value": "www.site2.com"},
                "total_urls": 100
            }
        }
        segments = get_segments_from_args(results, segments_idx)
        self.assertEquals(len(segments), 2)
        self.assertEquals(
            segments[0],
            {
                "segment": segments_idx[111],
                "nb_urls": 10
            }
        )
        self.assertEquals(
            segments[1],
            {
                "segment": segments_idx[222],
                "nb_urls": 8
            }
        )

    def test_load_segments_idx_from_file(self):
        f = StringIO()
        f.write('host="www.site.com"\t{"field": "host", "value": "www.site.com"}\t111\t10\n')
        f.write('host="www.site2.com"\t{"field": "host", "value": "www.site2.com"}\t42\t100\n')
        f.seek(0)
        segments = load_segments_idx_from_file(f)
        self.assertEquals(segments.keys(), [42, 111])
        self.assertEquals(
            segments[42],
            {
                "human": "host=\"www.site2.com\"",
                "query": {"field": "host", "value": "www.site2.com"},
                "total_urls": 100
            }
        )
        self.assertEquals(
            segments[111],
            {
                "human": "host=\"www.site.com\"",
                "query": {"field": "host", "value": "www.site.com"},
                "total_urls": 10
            }
        )
