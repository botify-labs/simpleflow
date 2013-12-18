import unittest
from mock import MagicMock

from cdf.streams.stream_factory import (HostStreamFactory,
                                        PathStreamFactory,
                                        QueryStringStreamFactory,
                                        MetadataStreamFactory)


class TestHostStreamFactory(unittest.TestCase):
    def test_nominal_case(self):
        stream_factory = MagicMock()
        urlids = [
            (0, "http", "www.foo.com"),
            (1, "http", "www.bar.com"),
            (3, "http", "www.bar.com"),
        ]
        stream_factory.get_stream.return_value = iter(urlids)
        stream_factory.get_max_crawled_urlid.return_value = 1

        path = None
        host_stream_factory = HostStreamFactory(path)
        host_stream_factory.set_stream_factory(stream_factory)

        #urlid 3 is not returned since it has not been crawled
        expected_result = [
            (0, "www.foo.com"),
            (1, "www.bar.com")
        ]

        self.assertEqual(expected_result,
                         list(host_stream_factory.get_stream()))


class TestPathStreamFactory(unittest.TestCase):
    def test_nominal_case(self):
        stream_factory = MagicMock()
        urlids = [
            (0, "http", "www.foo.com", "/"),
            (1, "http", "www.foo.com", "/bar"),
            (3, "http", "www.foo.com", "/bar/baz"),
        ]
        stream_factory.get_stream.return_value = iter(urlids)
        stream_factory.get_max_crawled_urlid.return_value = 1

        path = None
        path_stream_factory = PathStreamFactory(path)
        path_stream_factory.set_stream_factory(stream_factory)

        #urlid 3 is not returned since it has not been crawled
        expected_result = [
            (0, "/"),
            (1, "/bar")
        ]

        self.assertEqual(expected_result,
                         list(path_stream_factory.get_stream()))


class TestQueryStringStreamFactory(unittest.TestCase):
    def test_nominal_case(self):
        stream_factory = MagicMock()
        urlids = [
            (0, "http", "www.foo.com", "/", "?foo=1"),
            (1, "http", "www.foo.com", "/"),
            (2, "http", "www.foo.com", "/", "?foo=bar&baz=2"),
            (3, "http", "www.foo.com", "/", "?foo=2"),
        ]
        stream_factory.get_stream.return_value = iter(urlids)
        stream_factory.get_max_crawled_urlid.return_value = 2

        path = None
        qs_stream_factory = QueryStringStreamFactory(path)
        qs_stream_factory.set_stream_factory(stream_factory)

        #urlid 1 is not returned since it has no query string
        #urlid 3 is not returned since it has not been crawled
        expected_result = [
            (0, {"foo": ["1"]}),
            (2, {"foo": ["bar"], "baz": ["2"]})
        ]

        self.assertEqual(expected_result,
                         list(qs_stream_factory.get_stream()))


class TestMetadataStreamFactory(unittest.TestCase):
    def setUp(self):
        self.stream_factory = MagicMock()
        #setting hash to None since they should not be used in the test
        urlcontents = [
            (0, 1, None, "title"),
            (0, 3, None, "first h2"),
            (0, 3, None, "second h2"),
            (1, 1, None, "title"),
            (1, 2, None, "h1"),
            (1, 3, None, "h2"),
            (3, 1, None, "title"),
        ]
        self.stream_factory.get_stream.return_value = iter(urlcontents)
        self.stream_factory.get_max_crawled_urlid.return_value = 2

    def test_nominal_case_h1(self):
        path = None
        content_type = "h1"
        metadata_stream_factory = MetadataStreamFactory(path, content_type)
        metadata_stream_factory.set_stream_factory(self.stream_factory)

        #urlid 0 is not returned since it has no h1
        #urlid 3 is not returned since it has not been crawled
        expected_result = [
            (1, ["h1"])
        ]

        self.assertEqual(expected_result,
                         list(metadata_stream_factory.get_stream()))

    def test_nominal_case_h2(self):
        path = None
        content_type = "h2"
        metadata_stream_factory = MetadataStreamFactory(path, content_type)
        metadata_stream_factory.set_stream_factory(self.stream_factory)

        #urlid 3 is not returned since it has not been crawled
        expected_result = [
            (0, ["first h2", "second h2"]),
            (1, ["h2"])
        ]

        self.assertEqual(expected_result,
                         list(metadata_stream_factory.get_stream()))
