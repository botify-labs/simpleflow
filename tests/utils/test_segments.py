import unittest
from StringIO import StringIO

from cdf.utils.segments import (
    load_segments_from_files,
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
        segments_def = [
            {
                "human": "host=\"www.site.com\"",
                "query": {"field": "host", "value": "www.site.com"},
                "total_urls": 200,
                "hash": 111
            },
            {
                "human": "host=\"www.site2.com\"",
                "query": {"field": "host", "value": "www.site2.com"},
                "total_urls": 100,
                "hash": 222
            }
        ]
        segments = get_segments_from_args(results, segments_def)
        self.assertEquals(len(segments), 2)
        self.assertEquals(
            segments[0],
            {
                "segment": {
                    "human": segments_def[0]["human"],
                    "query": segments_def[0]["query"],
                    "total_urls": segments_def[0]["total_urls"],
                },
                "nb_urls": 10
            }
        )
        self.assertEquals(
            segments[1],
            {
                "segment": {
                    "human": segments_def[1]["human"],
                    "query": segments_def[1]["query"],
                    "total_urls": segments_def[1]["total_urls"],
                },
                "nb_urls": 8
            }
        )

    def test_load_segments_from_files(self):
        # Write segments names
        f_names = StringIO()
        f_names.write('host="www.site.com"\t{"field": "host", "value": "www.site.com"}\t111\t10\n')
        f_names.write('host="www.site2.com"\t{"field": "host", "value": "www.site2.com"}\t42\t100\n')
        f_names.seek(0)

        # Write relationships
        f_rel = StringIO()
        f_rel.write("111\t42\n")
        f_rel.seek(0)

        segments = load_segments_from_files(f_names, f_rel)
        self.assertEquals(len(segments), 2)
        self.assertEquals(
            segments[0],
            {
                "human": "host=\"www.site.com\"",
                "query": {"field": "host", "value": "www.site.com"},
                "total_urls": 10,
                "hash": 111,
                "parent": None,
                "children": [42]
            }
        )
        self.assertEquals(
            segments[1],
            {
                "human": "host=\"www.site2.com\"",
                "query": {"field": "host", "value": "www.site2.com"},
                "total_urls": 100,
                "hash": 42,
                "parent": 111,
                "children": []
            }
        )
