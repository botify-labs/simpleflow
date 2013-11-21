import unittest
import gzip
import os
import re

from mock import patch
from boto.exception import S3ResponseError

from cdf.log import logger
from cdf.streams.utils import split_file
from cdf.collections.urls.utils import get_part_id

import cdf.tasks.url_data as ud
import cdf.tasks.suggest.clusters as clusters
import cdf.tasks.intermediary_files as im
import cdf.tasks.aggregators as agg

CRAWL_ID = 0
S3_URI = ''

MOCK_CRAWL_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'mock')
TEST_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'test')
RESULT_DIR = os.path.join(TEST_DIR, 'crawl_%d' % CRAWL_ID)


def split_partition(input_file,
                    dest_dir,
                    first_part_size=4,
                    part_size=2):
    """Split a file in several partitions, according to
    `first_part_size` and `part_size`

    Output partitions are gzipped
    """
    current_part = 0
    output_path = os.path.join(dest_dir,
                               os.path.basename(input_file) + '.{}.gz')
    _in = open(input_file, 'rb')
    _out = gzip.open(output_path.format(current_part), 'w')

    for line in split_file(_in):
        # check the part id
        url_id = int(line[0])
        part = get_part_id(url_id, first_part_size, part_size)

        # a new part file is needed
        if part != current_part:
            # this is safe b/c url_id is ordered
            current_part = part
            _out.close()
            _out = gzip.open(output_path.format(current_part), 'w')

        _out.write('\t'.join(line) + '\n')
    _out.close()


def generate_inlink_file(outlink_file, inlink_file):
    """Reverse `urllinks`"""
    outlink = open(outlink_file, 'r')
    buffer = []

    for src, type, mask, dest, ext in split_file(outlink):
        is_internal = int(dest) > 0
        if is_internal:
            buffer.append([dest, type, mask, src])
    outlink.close()

    inlink = open(inlink_file, 'w')
    # sorted on dest
    for line in sorted(buffer, key=lambda x: int(x[0])):
        inlink.write('\t'.join(line) + '\n')
    inlink.close()


def list_result_files(dir, regexp):
    return [f for f in os.listdir(dir) if re.match(regexp, f)]


# Mock s3 module
def _list_local_files(input_dir, regexp):
    """List all files that satisfy the given regexp"""
    result = []

    def listdir(input_dir):
        """Recursively list all files under `input_dir`"""
        files = []
        for dir, _, filenames in os.walk(input_dir):
            files.extend([os.path.join(dir, f) for f in filenames])
        return files

    for f in listdir(input_dir):
        # exclude the top folder for regexp matching
        to_match = f[len(input_dir) + 1:]
        # regexp is a string, try match it
        if isinstance(regexp, str) and re.match(regexp, to_match):
            result.append(f)
        # regexp is a list of string, try match anyone of it
        elif isinstance(regexp, (list, tuple)):
            if any(re.match(r, to_match) for r in regexp):
                result.append(f)

    return result


def _mock_push_file(s3_uri, filename):
    """Verify that the file indeed exists"""
    if not os.path.exists(filename):
        raise Exception('{} file does not exists'.format(filename))


def _mock_push_content(s3_uri, content):
    """No push to s3"""
    pass


def _mock_fetch_file(s3_uri, dest_path, force_fetch, lock=True):
    """A mock fetch that merely check if the file exists locally"""
    filename = os.path.basename(dest_path)
    if not os.path.exists(dest_path):
        if not os.path.exists(os.path.join(TEST_DIR, filename)):
            # this should be managed correctly by tasks
            raise S3ResponseError('', '{} not found'.format(dest_path))
        else:
            return os.path.join(TEST_DIR, filename), True
    else:
        return dest_path, True


def _mock_fetch_files(s3_uri, dest_dir,
                      regexp=None, force_fetch=True, lock=True):
    local_files = _list_local_files(dest_dir, regexp)
    local_files.extend(_list_local_files(TEST_DIR, regexp))

    # it could return an empty list, this is managed by the tasks
    # that use it
    return [(f, True) for f in local_files]


def _mock_nb_parts(s3_uri):
    files = _list_local_files(TEST_DIR, 'urlids.txt.([0-9]+)')
    return len(files)


class MockIntergrationTest(unittest.TestCase):
    @classmethod
    @patch('cdf.utils.s3.push_file', _mock_push_file)
    @patch('cdf.utils.s3.push_content', _mock_push_content)
    @patch('cdf.utils.s3.fetch_file', _mock_fetch_file)
    @patch('cdf.utils.s3.fetch_files', _mock_fetch_files)
    @patch('cdf.utils.remote_files.nb_parts_from_crawl_location', _mock_nb_parts)
    def setUpClass(cls):
        # generate inlink file
        generate_inlink_file(os.path.join(MOCK_CRAWL_DIR, 'urllinks.txt'),
                             os.path.join(MOCK_CRAWL_DIR, 'urlinlinks.txt'))

        # prepare test folder
        crawl_dir = os.path.join(TEST_DIR, 'crawl_%d' % CRAWL_ID)
        if not os.path.exists(crawl_dir):
            os.makedirs(crawl_dir)

        # split and gzip mock crawl files
        for file in os.listdir(MOCK_CRAWL_DIR):
            split_partition(os.path.join(MOCK_CRAWL_DIR, file), TEST_DIR)

        # reload modules, mocks need this to take effect
        reload(im)
        reload(agg)
        reload(clusters)
        reload(ud)

        # launch cdf's analyse process
        force_fetch = False

        # figure out number of partitions
        from cdf.utils.remote_files import nb_parts_from_crawl_location
        parts = nb_parts_from_crawl_location(S3_URI)

        # bad link
        im.make_bad_link_file(CRAWL_ID, S3_URI, 4, 2, tmp_dir_prefix=TEST_DIR)

        # aggregate bad links on (url, http_code)
        for part_id in xrange(0, parts):
            im.make_bad_link_counter_file(CRAWL_ID, S3_URI, part_id, tmp_dir_prefix=TEST_DIR)

        # metadata duplication detection
        im.make_metadata_duplicates_file(CRAWL_ID, S3_URI, 4, 2,
                                         tmp_dir_prefix=TEST_DIR)

        # clustering
        clusters.compute_mixed_clusters(CRAWL_ID, S3_URI, 4, 2,
                                        tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)

        # aggregation for each partition
        for part_id in xrange(0, parts):
            logger.info("Compute inlinks counter file")
            im.make_links_counter_file(CRAWL_ID, S3_URI, part_id, "in",
                                       tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)
            logger.info("Compute outlinks counter file")
            im.make_links_counter_file(CRAWL_ID, S3_URI, part_id, "out",
                                       tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)
            agg.compute_aggregators_from_part_id(CRAWL_ID, S3_URI, part_id,
                                                 tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)

        # consolidation of partitions
        agg.consolidate_aggregators(CRAWL_ID, S3_URI,
                                    tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)

        # TODO(darkjh) mock ElasticSearch
        es_location = 'http://localhost:9200'
        es_index = 'integration-test'
        es_doc_type = 'crawls'
        ud.prepare_crawl_index(CRAWL_ID, es_location, es_index, es_doc_type)
        for part_id in xrange(0, parts):
            ud.push_urls_to_elastic_search(CRAWL_ID, part_id, S3_URI,
                                           es_location, es_index, es_doc_type,
                                           tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)


    @classmethod
    def tearDownClass(cls):
        # TODO(darkjh) delete generated files
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def assert_part_files(self, file_pattern,
                          expected_parts):
        """Assert that with the given `file_pattern`, only files for
        `expected_parts` should be created

        :param file_pattern should be of form 'some_name.txt.{}.gz'
        :param expected_parts is a list/tuple of expected part numbers
        """
        file_regexp = file_pattern.format('*')
        self.assertEqual(len(expected_parts),
                         len(list_result_files(RESULT_DIR, file_regexp)))
        for part in expected_parts:
            self.assertEqual([file_pattern.format(part)],
                             list_result_files(RESULT_DIR, file_pattern.format(part)))

    def test_in_links_files(self):
        pattern = 'url_in_links_counters.txt.{}.gz'
        self.assert_part_files(pattern, [0, 2, 3, 4, 5])

    def test_in_canonicals_files(self):
        # TODO(darkjh) issue 128
        pass

    def test_in_redirects_files(self):
        pattern = 'url_in_redirect_counters.txt.{}.gz'
        self.assert_part_files(pattern, [0, 2, 4])

    def test_out_links_files(self):
        pattern = 'url_out_links_counters.txt.{}.gz'
        self.assert_part_files(pattern, [0, 4])

    def test_out_redirects_files(self):
        pattern = 'url_out_redirect_counters.txt.{}.gz'
        self.assert_part_files(pattern, [2, 5])

    def test_out_canonicals_files(self):
        pattern = 'url_out_canonical_counters.txt.{}.gz'
        self.assert_part_files(pattern, [0, 1])

    def test_bad_links_files(self):
        pattern = 'urlbadlinks.txt.{}.gz'
        self.assert_part_files(pattern, [0, 4])

    def test_metadata_duplication_files(self):
        pattern = 'urlcontentsduplicate.txt.{}.gz'
        self.assert_part_files(pattern, [0, 1, 4])