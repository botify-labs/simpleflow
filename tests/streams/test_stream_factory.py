import unittest
import StringIO

from mock import MagicMock, patch

from cdf.exceptions import MalformedFileNameError
from cdf.core.streams.stream_factory import (get_part_id_from_filename,
                                             FileStreamFactory,
                                             ProtocolStreamFactory,
                                             HostStreamFactory,
                                             PathStreamFactory,
                                             QueryStringStreamFactory,
                                             MetadataStreamFactory,
                                             get_max_crawled_urlid,
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
                          None,
                          "unknown_content")

    def test_get_file_regexp(self):
        dirpath = None
        content = "urlids"
        crawler_metakeys = None
        stream_factory = FileStreamFactory(dirpath, content, crawler_metakeys)
        self.assertEqual("urlids.txt.*.gz",
                         stream_factory._get_file_regexp().pattern)

        part_id = 1
        stream_factory = FileStreamFactory(dirpath,
                                           content,
                                           crawler_metakeys,
                                           part_id)
        self.assertEqual("urlids.txt.1.gz",
                         stream_factory._get_file_regexp().pattern)

    def test_get_file_list_nominal_case(self):
        crawler_metakeys = {
            "urlinfos": ["crawl-result/urlinfos.txt.0.gz",
                         "crawl-result/urlinfos.txt.1.gz"]
        }
        dirpath = "/tmp/crawl_data"
        content = "urlinfos"
        stream_factory = FileStreamFactory(dirpath, content, crawler_metakeys)

        expected_result = ["/tmp/crawl_data/urlinfos.txt.0.gz",
                           "/tmp/crawl_data/urlinfos.txt.1.gz"]
        self.assertEquals(expected_result,
                          stream_factory._get_file_list(crawler_metakeys))

    def test_get_file_list_incomplete_crawler_metakeys(self):
        crawler_metakeys = {}
        dirpath = "/tmp/crawl_data"
        content = "urlinfos"
        stream_factory = FileStreamFactory(dirpath, content, crawler_metakeys)
        self.assertEquals([], stream_factory._get_file_list(crawler_metakeys))

    def test_get_stream_from_file(self):
        dirpath = None
        content = "urlids"
        crawler_metakeys = None
        stream_factory = FileStreamFactory(dirpath, content, crawler_metakeys)
        #fake file object
        file_content = ("1\thttp\twww.foo.com\t/bar\t?param=value\n"
                        "3\thttp\twww.foo.com\t/bar/baz")
        file = StringIO.StringIO(file_content)

        expected_result = [[1, "http", "www.foo.com", "/bar", "?param=value"],
                           [3, "http", "www.foo.com", "/bar/baz"]]
        actual_result = stream_factory._get_stream_from_file(file)
        self.assertEqual(expected_result, list(actual_result))

    @patch('gzip.open')
    def test_get_stream(self, gzip_open_mock):
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
        crawler_metakeys = {"max_uid_we_crawled": 3,
                            "urlids": ["/tmp/crawl-1/urlids.txt.2.gz",
                                       "/tmp/crawl-1/urlids.txt.0.gz"]}
        file_stream_factory = FileStreamFactory("/tmp/crawl-1",
                                                "urlids",
                                                crawler_metakeys)
        #result stream should respect part_id order
        expected_result = [[1, "http", "www.foo.com"],
                           [3, "http", "www.bar.com"]]
        self.assertEqual(expected_result,
                         list(file_stream_factory.get_stream()))

    def test_get_stream_missing_file(self):
        #actual test
        crawler_metakeys = {"max_uid_we_crawled": 3,
                            "urlids": ["/unexisting_dir/urlids.txt.0.gz"]}
        file_stream_factory = FileStreamFactory("/tmp/crawl-1",
                                                "urlids",
                                                crawler_metakeys)
        self.assertRaises(IOError,
                          file_stream_factory.get_stream)

class TestProtocolStreamFactory(unittest.TestCase):
    def test_nominal_case(self):
        stream_factory = MagicMock()
        urlids = [
            (0, "http", "www.foo.com"),
            (1, "https", "www.bar.com"),
            (3, "https", "www.bar.com"),
        ]
        stream_factory.get_stream.return_value = iter(urlids)

        path = None
        crawler_metakeys = {"max_uid_we_crawled": 1}
        protocol_stream_factory = ProtocolStreamFactory(path, crawler_metakeys)
        protocol_stream_factory.set_file_stream_factory(stream_factory)

        #urlid 3 is not returned since it has not been crawled
        expected_result = [
            (0, "http"),
            (1, "https")
        ]

        self.assertEqual(expected_result,
                         list(protocol_stream_factory.get_stream()))



class TestHostStreamFactory(unittest.TestCase):
    def test_nominal_case(self):
        stream_factory = MagicMock()
        urlids = [
            (0, "http", "www.foo.com"),
            (1, "http", "www.bar.com"),
            (3, "http", "www.bar.com"),
        ]
        stream_factory.get_stream.return_value = iter(urlids)

        path = None
        crawler_metakeys = {"max_uid_we_crawled": 1}
        host_stream_factory = HostStreamFactory(path, crawler_metakeys)
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

        path = None
        crawler_metakeys = {"max_uid_we_crawled": 1}
        path_stream_factory = PathStreamFactory(path, crawler_metakeys)
        path_stream_factory.set_file_stream_factory(stream_factory)

        #urlid 3 is not returned since it has not been crawled
        expected_result = [
            (0, "/"),
            (1, "/bar")
        ]

        self.assertEqual(expected_result,
                         list(path_stream_factory.get_stream()))


class TestQueryStringStreamFactory(unittest.TestCase):
    def setUp(self):
        self.stream_factory = MagicMock()
        urlids = [
            (0, "http", "www.foo.com", "/", "?foo=1"),
            (1, "http", "www.foo.com", "/"),
            (2, "http", "www.foo.com", "/", "?foo=bar&baz=2"),
            (3, "http", "www.foo.com", "/", "?foo=2"),
        ]
        self.stream_factory.get_stream.return_value = iter(urlids)
        path = None
        crawler_metakeys = {"max_uid_we_crawled": 2}
        self.qs_stream_factory = QueryStringStreamFactory(path,
                                                          crawler_metakeys)
        self.qs_stream_factory.set_file_stream_factory(self.stream_factory)

    def test_nominal_case(self):

        #urlid 3 is not returned since it has not been crawled
        expected_result = [
            (0, {"foo": ["1"]}),
            (1, {}),
            (2, {"foo": ["bar"], "baz": ["2"]})
        ]

        self.assertEqual(expected_result,
                         list(self.qs_stream_factory.get_stream()))

    def test_do_not_parse_string(self):
        self.qs_stream_factory.parse_string = False

        #urlid 3 is not returned since it has not been crawled
        expected_result = [
            (0, "foo=1"),
            (1, ""),
            (2, "foo=bar&baz=2")
        ]

        self.assertEqual(expected_result,
                         list(self.qs_stream_factory.get_stream()))


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

        self.crawler_metakeys = {"max_uid_we_crawled": 2}

    def test_nominal_case_h1(self):
        path = None
        content_type = "h1"
        metadata_stream_factory = MetadataStreamFactory(path,
                                                        content_type,
                                                        self.crawler_metakeys)
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
        metadata_stream_factory = MetadataStreamFactory(path,
                                                        content_type,
                                                        self.crawler_metakeys)
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


class TestGetMaxCrawledUrlid(unittest.TestCase):
    def test_nominal_case(self):
        crawler_metakeys = {"max_uid_we_crawled": 2}
        self.assertEqual(2, get_max_crawled_urlid(crawler_metakeys))
