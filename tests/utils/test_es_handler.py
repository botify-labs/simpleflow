import unittest

from cdf.utils.es import EsHandler


class TestEsHandler(unittest.TestCase):
    def test_location(self):
        expected_host = ['localhost:9200']
        # with protocol
        location = 'http://localhost:9200'
        es = EsHandler(location, '', '')
        self.assertEqual(es.es_host, expected_host)

        # without protocol
        location = 'localhost:9200'
        es = EsHandler(location, '', '')
        self.assertEqual(es.es_host, expected_host)

    def test_multiple_hosts(self):
        expected = ['host1:9200', 'host2:9200']
        es = EsHandler(['host1:9200', 'host2:9200'], '', '')
        self.assertEqual(es.es_host, expected)