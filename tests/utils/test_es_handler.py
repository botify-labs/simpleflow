import unittest
import mock
from contextlib import nested
from functools import partial

import cdf.exceptions
from cdf.utils.es import EsHandler


class TestEsHandlerUrls(unittest.TestCase):
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
        hosts = ['host1:9200', 'host2:9200']
        expected = hosts
        es = EsHandler(hosts, '', '')
        self.assertEqual(es.es_host, expected)


class TestEsHandlerDns(unittest.TestCase):
    def setUp(self):
        from cdf.utils.discovery import DnsHostDiscovery

        self.EsHandler = partial(EsHandler, host_discovery=DnsHostDiscovery)

    def test_dns_single_record(self):
        ips = ['1.2.3.4']
        with mock.patch(
            'socket.gethostbyname_ex',
            lambda hostname: (hostname, (), ips),
        ):
            es = self.EsHandler('es.botify.com', 'fake_index', 'fake_doc_type')
            self.assertEquals(es.es_host, ips)

    def test_dns_multiple_records(self):
        ips = ['1.2.3.4', '5.6.7.8']
        with mock.patch(
            'socket.gethostbyname_ex',
            lambda hostname: (hostname, (), ips),
        ):
            es = self.EsHandler('es.botify.com', 'fake_index', 'fake_doc_type')
            self.assertEquals(es.es_host, ips)

    def test_dns_record_does_not_exist(self):
        import socket

        def host_does_not_exist(hostname):
            raise socket.gaierror(-5, 'No address associated with hostname')

        with nested(
            mock.patch('socket.gethostbyname_ex', host_does_not_exist),
            self.assertRaises(cdf.exceptions.HostDoesNotExist),
        ):
            self.EsHandler('es.botify.com', 'fake_index', 'fake_doc_type')
