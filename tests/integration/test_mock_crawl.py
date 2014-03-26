import unittest
import os
import re
import itertools
import shutil
from elasticsearch import Elasticsearch

from mock import patch
from boto.exception import S3ResponseError

from cdf.log import logger
import cdf.tasks.url_data as ud
import cdf.tasks.suggest.clusters as clusters
import cdf.tasks.intermediary_files as im
import cdf.tasks.aggregators as agg
from test_utils import split_partition, list_result_files, generate_inlink_file, get_stream_from_file

CRAWL_ID = 0
S3_URI = ''

MOCK_CRAWL_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'mock')
TEST_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'test')
RESULT_DIR = os.path.join(TEST_DIR, 'crawl_%d' % CRAWL_ID)


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


def _mock_nb_parts(s3_uri, dirpath=None):
    files = _list_local_files(dirpath if dirpath else RESULT_DIR,
                              'urlids.txt.([0-9]+)')
    return len(files)


class MockIntegrationTest(unittest.TestCase):
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

        # copy `files.json`
        shutil.copy(os.path.join(MOCK_CRAWL_DIR, 'files.json'),
                    os.path.join(crawl_dir, 'files.json'))

        # split and gzip mock crawl files
        for file in _list_local_files(MOCK_CRAWL_DIR, 'url.*'):
            split_partition(os.path.join(MOCK_CRAWL_DIR, file), crawl_dir)

        # reload modules, mocks need this to take effect
        reload(im)
        reload(agg)
        reload(clusters)
        reload(ud)

        # launch cdf's analyse process
        force_fetch = True

        # figure out number of partitions
        from cdf.utils.remote_files import nb_parts_from_crawl_location
        parts = nb_parts_from_crawl_location(S3_URI, crawl_dir)

        # bad link
        im.make_bad_link_file(CRAWL_ID, TEST_DIR, 4, 2, tmp_dir=RESULT_DIR)

        # aggregate bad links on (url, http_code)
        for part_id in xrange(0, parts):
            im.make_bad_link_counter_file(CRAWL_ID, TEST_DIR, part_id, tmp_dir=RESULT_DIR)

        # metadata duplication detection
        im.make_metadata_duplicates_file(CRAWL_ID, TEST_DIR, 4, 2,
                                         tmp_dir=RESULT_DIR)

        # clustering
        clusters.compute_mixed_clusters(CRAWL_ID, TEST_DIR, 4, 2,
                                        tmp_dir=RESULT_DIR, force_fetch=force_fetch)

        # aggregation for each partition
        for part_id in xrange(0, parts):
            logger.info("Compute inlinks counter file")
            im.make_links_counter_file(CRAWL_ID, TEST_DIR, part_id, "in",
                                       tmp_dir=RESULT_DIR, force_fetch=force_fetch)
            logger.info("Compute outlinks counter file")
            im.make_links_counter_file(CRAWL_ID, TEST_DIR, part_id, "out",
                                       tmp_dir=RESULT_DIR, force_fetch=force_fetch)
            agg.compute_aggregators_from_part_id(CRAWL_ID, TEST_DIR, part_id,
                                                 tmp_dir=RESULT_DIR, force_fetch=force_fetch)

        # consolidation of partitions
        agg.consolidate_aggregators(CRAWL_ID, TEST_DIR,
                                    tmp_dir=RESULT_DIR, force_fetch=force_fetch)

        # TODO(darkjh) mock ElasticSearch
        es_location = 'http://localhost:9200'
        es_index = 'integration-test'
        es_doc_type = 'crawls'
        ud.prepare_crawl_index(CRAWL_ID, es_location, es_index, es_doc_type)
        for part_id in xrange(0, parts):
            ud.generate_documents(CRAWL_ID, part_id, TEST_DIR, force_fetch=force_fetch, tmp_dir=RESULT_DIR)
            ud.push_documents_to_elastic_search(TEST_DIR, part_id, es_location,
                                                es_index, es_doc_type,
                                                force_fetch=force_fetch, tmp_dir=RESULT_DIR)

    @classmethod
    def tearDownClass(cls):
        # delete created files
        shutil.rmtree(TEST_DIR)
        # delete ES index
        ES = Elasticsearch()
        ES.indices.delete('integration-test')
        ES.indices.refresh()

    def setUp(self):
        self.maxDiff = None

    def tearDown(self):
        pass

    def assert_part_files(self, file_pattern, expected_parts):
        """Assert that with the given `file_pattern`, only files for
        `expected_parts` should be created

        :param file_pattern: should be of form 'some_name.txt.{}.gz'
        :param expected_parts: is a list/tuple of expected part numbers
        """
        file_regexp = file_pattern.format('*')
        self.assertEqual(len(expected_parts),
                         len(list_result_files(RESULT_DIR, file_regexp)))
        for part in expected_parts:
            self.assertEqual([file_pattern.format(part)],
                             list_result_files(RESULT_DIR, file_pattern.format(part)))

    def assert_file_contents(self, file_pattern, expedted_contents):
        """Assert that result files contain the correct content

        :param file_pattern: should be of form 'some_name.txt.{}.gz'
        :param expedted_contents: order is NOT important
        """
        file_regexp = file_pattern.format('*')
        # list full path, need to open them
        files = list_result_files(RESULT_DIR, file_regexp, full_path=True)
        stream = itertools.chain(*map(get_stream_from_file, files))

        l = list(stream)
        self.assertItemsEqual(l, expedted_contents)

    def test_in_links_files(self):
        pattern = 'url_in_links_counters.txt.{}.gz'

        self.assert_part_files(pattern, [0, 2, 3, 5])

        expected_contents = [
            [2, ['follow'], 1, 1],
            [6, ['follow'], 1, 1],
            [6, ['link'], 1, 1],
            [8, ['follow'], 3, 1],
            [8, ['follow'], 1, 1],
            [8, ['link'], 2, 1],
            [9, ['follow'], 5, 1],
            [12, ['follow'], 1, 1]
        ]

        self.assert_file_contents(pattern, expected_contents)

    def test_in_canonicals_files(self):
        pattern = 'url_in_canonical_counters.txt.{}.gz'

        self.assert_part_files(pattern, [0])

        expected_contents = [
            [2, 3],
        ]
        self.assert_file_contents(pattern, expected_contents)

    def test_in_redirects_files(self):
        pattern = 'url_in_redirect_counters.txt.{}.gz'

        self.assert_part_files(pattern, [0, 2, 4])

        expected_contents = [
            [2, 1],
            [7, 1],
            [10, 1]
        ]

        self.assert_file_contents(pattern, expected_contents)

    def test_out_links_files(self):
        pattern = 'url_out_links_counters.txt.{}.gz'

        self.assert_part_files(pattern, [0, 4])

        expectec_contents = [
            [1, ['follow'], True, 10, 4],
            [1, ['robots', 'meta', 'link'], True, 3, 1],
            [1, ['follow'], True, 1, 1],
            [1, ['robots', 'meta'], True, 2, 1],
            [1, ['robots', 'link'], True, 2, 1],
            [1, ['link'], True, 3, 2],
            [11, ['follow'], True, 1, 1],
            [11, ['link'], False, 2, 1],
            [11, ['follow'], False, 3, 1]
        ]

        self.assert_file_contents(pattern, expectec_contents)

    def test_out_redirects_files(self):
        pattern = 'url_out_redirect_counters.txt.{}.gz'

        self.assert_part_files(pattern, [2, 5])

        expected_contents = [
            [6, 1],
            [7, 1],
            [12, 1]
        ]

        self.assert_file_contents(pattern, expected_contents)

    def test_out_canonicals_files(self):
        pattern = 'url_out_canonical_counters.txt.{}.gz'

        self.assert_part_files(pattern, [0, 1])

        expected_contents = [
            [1, 1],
            [2, 1],
            [3, 0],
            [4, 0],
            [5, 0],
        ]

        self.assert_file_contents(pattern, expected_contents)

    def test_bad_links_files(self):
        pattern = 'urlbadlinks.txt.{}.gz'
        self.assert_part_files(pattern, [0, 4])


        expected_contents = []
        # use list.extend and * for repeats
        expected_contents.extend([[1, 8, 401]] * 10)
        expected_contents.extend([[1, 6, 301]] * 2)
        expected_contents.extend([[1, 9, 501]] * 5)
        expected_contents.extend([[1, 10, 404]] * 3)
        expected_contents.append([11, 12, 303])

        self.assert_file_contents(pattern, expected_contents)

    def test_metadata_duplication_files(self):
        pattern = 'urlcontentsduplicate.txt.{}.gz'

        self.assert_part_files(pattern, [0, 1, 4])

        expected_contents = [
            [1, 1, 1, 3, 1, [2, 3]],
            [1, 2, 2, 2, 1, [2]],
            [1, 4, 1, 2, 1, [4]],
            [2, 1, 1, 3, 0, [1, 3]],
            [2, 2, 2, 2, 0, [1]],
            [2, 4, 1, 2, 1, [5]],
            [3, 1, 1, 3, 0, [1, 2]],
            [3, 2, 1, 0, 1, []],
            [4, 1, 1, 0, 1, []],
            [4, 4, 1, 2, 0, [1]],
            [5, 2, 1, 0, 1, []],
            [5, 4, 1, 2, 0, [2]],
            [11, 1, 1, 0, 1, []]
        ]

        self.assert_file_contents(pattern, expected_contents)