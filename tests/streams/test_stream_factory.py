import unittest
from mock import MagicMock, patch

import StringIO

from cdf.exceptions import MalformedFileNameError
from cdf.streams.stream_factory import (get_part_id_from_filename,
                                        FileStreamFactory,
                                        HostStreamFactory,
                                        PathStreamFactory,
                                        QueryStringStreamFactory,
                                        MetadataStreamFactory,
                                        _get_nb_crawled_urls_from_stream)


class TestGetPartIdFromFileName(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual(0, get_part_id_from_filename("urlcontents.txt.0.gz"))
        self.assertEqual(10, get_part_id_from_filename("urlcontents.txt.10.gz"))
        self.assertEqual(0, get_part_id_from_filename("/tmp/urlcontents.txt.0.gz"))

    def test_malformed_filename(self):
        self.assertRaises(MalformedFileNameError,
                          get_part_id_from_filename,
                          "urlcontents.txt.gz")

        self.assertRaises(MalformedFileNameError,
                          get_part_id_from_filename,
                          "urlcontents.txt.-1.gz")


class TestStreamFactory(unittest.TestCase):
    def test_constructor(self):
        #test that the constructor raises on unknow content
        self.assertRaises(Exception,
                          FileStreamFactory,
                          "/tmp",
                          "unknown_content")

    def test_get_file_regexp(self):
        dirpath = None
        content = "urlids"
        stream_factory = FileStreamFactory(dirpath, content)
        self.assertEqual("urlids.txt.*.gz",
                         stream_factory._get_file_regexp().pattern)

        part_id = 1
        stream_factory = FileStreamFactory(dirpath, content, part_id)
        self.assertEqual("urlids.txt.1.gz",
                         stream_factory._get_file_regexp().pattern)

    def test_get_file_list_nominal_case(self):
        json_content = {
            "urlinfos": ["crawl-result/urlinfos.txt.0.gz",
                         "crawl-result/urlinfos.txt.1.gz"]
        }
        dirpath = "/tmp/crawl_data"
        content = "urlinfos"
        stream_factory = FileStreamFactory(dirpath, content)

        expected_result = ["/tmp/crawl_data/urlinfos.txt.0.gz",
                           "/tmp/crawl_data/urlinfos.txt.1.gz"]
        self.assertEquals(expected_result,
                          stream_factory._get_file_list(json_content))

    def test_get_file_list_incomplete_json(self):
        json_content = {}
        dirpath = "/tmp/crawl_data"
        content = "urlinfos"
        stream_factory = FileStreamFactory(dirpath, content)
        self.assertEquals([], stream_factory._get_file_list(json_content))

    def test_get_stream_from_file(self):
        dirpath = None
        content = "urlids"
        stream_factory = FileStreamFactory(dirpath, content)
        #fake file object
        file_content = ("1\thttp\twww.foo.com\t/bar\t?param=value\n"
                        "3\thttp\twww.foo.com\t/bar/baz")
        file = StringIO.StringIO(file_content)

        expected_result = [[1, "http", "www.foo.com", "/bar", "?param=value"],
                           [3, "http", "www.foo.com", "/bar/baz"]]
        actual_result = stream_factory._get_stream_from_file(file)
        self.assertEqual(expected_result, list(actual_result))

    #patch get_json_file_content to avoid file creation
    @patch("cdf.streams.stream_factory.FileStreamFactory._get_json_file_content",
           new=lambda x: {"max_uid_we_crawled": 3})
    def test_get_max_crawled_urlid(self):
        dirpath = None
        content = "urlids"
        stream_factory = FileStreamFactory(dirpath, content)
        self.assertEqual(3, stream_factory.get_max_crawled_urlid())

    @patch("cdf.streams.stream_factory.FileStreamFactory._get_json_file_content")
    @patch('gzip.open')
    def test_get_stream(self, gzip_open_mock, json_mock):
        #note that urlids are not sorted by part_id
        json_mock.return_value = {"max_uid_we_crawled": 3,
                                  "urlids": ["/tmp/crawl-1/urlids.txt.2.gz",
                                             "/tmp/crawl-1/urlids.txt.0.gz"]}

        #mock gzip.open
        def side_effect(*args, **kwargs):
            filepath = args[0]
            file_contents = {
                "/tmp/crawl-1/urlids.txt.0.gz": ['1\thttp\twww.foo.com\n'],
                "/tmp/crawl-1/urlids.txt.2.gz": ['3\thttp\twww.bar.com\n']
            }
            mock = MagicMock()
            mock.__iter__.return_value = iter(file_contents[filepath])
            return mock
        gzip_open_mock.side_effect = side_effect

        #actual test
        file_stream_factory = FileStreamFactory("/tmp/crawl-1", "urlids")
        #result stream should respect part_id order
        expected_result = [[1, "http", "www.foo.com"],
                           [3, "http", "www.bar.com"]]
        self.assertEqual(expected_result,
                         list(file_stream_factory.get_stream()))

    @patch("cdf.streams.stream_factory.FileStreamFactory._get_json_file_content")
    def test_get_stream_missing_file(self, json_mock):
        json_mock.return_value = {"max_uid_we_crawled": 3,
                                  "urlids": ["/unexisting_dir/urlids.txt.0.gz"]
                                  }

        #actual test
        file_stream_factory = FileStreamFactory("/tmp/crawl-1", "urlids")
        self.assertRaises(IOError,
                          file_stream_factory.get_stream)


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
        host_stream_factory.set_file_stream_factory(stream_factory)

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
        path_stream_factory.set_file_stream_factory(stream_factory)

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
        qs_stream_factory.set_file_stream_factory(stream_factory)

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
        metadata_stream_factory.set_file_stream_factory(self.stream_factory)

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
        metadata_stream_factory.set_file_stream_factory(self.stream_factory)

        #urlid 3 is not returned since it has not been crawled
        expected_result = [
            (0, ["first h2", "second h2"]),
            (1, ["h2"])
        ]

        self.assertEqual(expected_result,
                         list(metadata_stream_factory.get_stream()))


class TestGetNumberPagesFromStream(unittest.TestCase):
    def test_nominal_case(self):
        urlinfos = [
            (1, 0, "text/html", 0, 0, 200),
            (2, 0, "text/html", 0, 0, 0),
            (3, 0, "text/html", 0, 0, 200),
            (4, 0, "text/html", 0, 0, 200),
        ]
        self.assertEqual(2, _get_nb_crawled_urls_from_stream(iter(urlinfos),
                                                             3))
        self.assertEqual(3, _get_nb_crawled_urls_from_stream(iter(urlinfos),
                                                             5))
