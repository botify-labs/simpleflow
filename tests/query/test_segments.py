import unittest
from StringIO import StringIO

from cdf.query.segments import load_segments_idx_from_file


class TestSegments(unittest.TestCase):
    def setUp(self):
        pass

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
