# -*- coding:utf-8 -*-
import unittest
import tempfile
import os


from cdf.urls_documents import UrlsDocuments


class TestUrlsDocuments(unittest.TestCase):

    def setUp(self):
        self.url_file = tempfile.mkstemp(prefix='cdf_test')[1]
        self.infos_file = tempfile.mkstemp(prefix='cdf_test')[1]
        self.contents_file = tempfile.mkstemp(prefix='cdf_test')[1]

    def tearDown(self):
        """
        os.remove(self.url_file.name)
        os.remove(self.infos_file.name)
        os.remove(self.contents_file.name)
        """
    def test_simple(self):
        f = open(self.url_file, 'w')
        f.write('1\thttp\twww.site.com\t/path/name.html\t\t\n')
        f.write('2\thttp\twww.site.com\t/path/name2.html\t\t\n')
        f.close()

        f = open(self.infos_file, 'w')
        f.write('1\t0\t6951326\t200\t89314\t303\t456\t0\n')
        f.write('2\t0\t6951327\t301\t1000\t303\t456\t0\n')
        f.close()

        print self.url_file

        u = UrlsDocuments(self.url_file, self.infos_file, self.contents_file)
        urls = [k for k in u.next()]
        print urls
