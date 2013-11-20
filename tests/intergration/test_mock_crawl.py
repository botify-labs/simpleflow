import unittest
import gzip
import os
import re

from mock import patch
from boto.exception import S3ResponseError

from cdf.log import logger
from cdf.streams.utils import split_file

from cdf.tasks.url_data import prepare_crawl_index, push_urls_to_elastic_search
import cdf.tasks.suggest.clusters as clusters
import cdf.tasks.intermediary_files as im
import cdf.tasks.aggregators as agg


MOCK_CRAWL_DIR = os.path.join(os.path.curdir, 'mock')
TEST_DIR = os.path.join(os.path.curdir, 'test')

CRAWL_ID = 0
S3_URI = ''


# TODO bug in intermediary_files
def split_partition(input_file,
                    dest_dir,
                    first_part_size=4,
                    part_size=2):
    """Split a plain text crawl file into gzipped partitions

    The 2 size params controls the url ids in the partition.
    For example, the first partition (index 0), it contains data records
    from url 1 up to the url id which equals `first_part_size`

    Outputs are write to `dest_dir`
    """
    current_part = 0
    output_name = os.path.join(dest_dir,
                               os.path.basename(input_file) + '.{}.gz')
    _in = open(input_file, 'rb')
    _out = gzip.open(output_name.format(current_part), 'w')

    for line in split_file(_in):
        # check the part id
        url_id = int(line[0])
        if (current_part == 0 and url_id > first_part_size) or \
                (current_part > 0 and
                         (url_id - first_part_size - (current_part - 1) * part_size) > part_size):
            _out.close()
            current_part += 1
            _out = gzip.open(output_name.format(current_part), 'w')
        _out.write('\t'.join(line) + '\n')
    _out.close()


def generate_inlink_file(outlink_file, inlink_file):
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


# Mock s3 module
def _list_local_files(input_dir, regexp):
    """List all files that satisfy the given regexp"""
    result = []
    for f in os.listdir(input_dir):
        # regexp is a string, try match it
        if isinstance(regexp, str) and re.match(regexp, f):
            result.append(os.path.join(input_dir, f))
        # regexp is a list of string, try match anyone of it
        elif isinstance(regexp, (list, tuple)):
            if any(re.match(r, f) for r in regexp):
                result.append(os.path.join(input_dir, f))

    return result


def _mock_push_file(s3_uri, filename):
    """No push to s3"""
    pass


def _mock_push_content(s3_uri, content):
    """No push to s3"""
    pass


def _mock_fetch_file(s3_uri, dest_path, force_fetch, lock=True):
    """A mock fetch that merely check if the file exists locally"""
    if not os.path.exists(dest_path):
        raise S3ResponseError('', '{} not found'.format(dest_path))
    return dest_path, True


def _mock_fetch_files(s3_uri, dest_dir,
                      regexp=None, force_fetch=True, lock=True):
    local_files = _list_local_files(dest_dir, regexp)
    if len(local_files) == 0:
        local_files = _list_local_files(TEST_DIR, regexp)

    # it could return an empty list, this is managed by the tasks
    # that use it
    return [(f, True) for f in local_files]


def _mock_nb_parts(s3_uri):
    files = _list_local_files(TEST_DIR, 'urlids.txt.([0-9]+)')
    return len(files)


class MockIntergrationTest(unittest.TestCase):
    def setUp(self):
        generate_inlink_file(os.path.join(MOCK_CRAWL_DIR, 'urllinks.txt'),
                             os.path.join(MOCK_CRAWL_DIR, 'urlinlinks.txt'))

        crawl_dir = os.path.join(TEST_DIR, 'crawl_%d' % CRAWL_ID)
        if not os.path.exists(crawl_dir):
            os.makedirs(crawl_dir)

        for file in os.listdir(MOCK_CRAWL_DIR):
            split_partition(os.path.join(MOCK_CRAWL_DIR, file), TEST_DIR)

    def tearDown(self):
        # delete generated files
        pass

    @patch('cdf.utils.s3.push_file', _mock_push_file)
    @patch('cdf.utils.s3.push_content', _mock_push_content)
    @patch('cdf.utils.s3.fetch_file', _mock_fetch_file)
    @patch('cdf.utils.s3.fetch_files', _mock_fetch_files)
    @patch('cdf.utils.remote_files.nb_parts_from_crawl_location', _mock_nb_parts)
    def test_mock_crawl(self):
        from cdf.utils.remote_files import nb_parts_from_crawl_location

        # reload modules, let mocks take effect
        reload(im)
        reload(agg)
        reload(clusters)

        force_fetch = False
        parts = nb_parts_from_crawl_location(S3_URI)

        im.make_bad_link_file(CRAWL_ID, S3_URI, 4, 2, tmp_dir_prefix=TEST_DIR)

        for part_id in xrange(0, parts):
            im.make_bad_link_counter_file(CRAWL_ID, S3_URI, part_id, tmp_dir_prefix=TEST_DIR)

        im.make_metadata_duplicates_file(CRAWL_ID, S3_URI, 4, 2,
                                         tmp_dir_prefix=TEST_DIR)

        clusters.compute_mixed_clusters(CRAWL_ID, S3_URI, 4, 2,
                                        tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)

        for part_id in xrange(0, parts):
            logger.info("Compute inlinks counter file")
            im.make_links_counter_file(CRAWL_ID, S3_URI, part_id, "in",
                                       tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)
            logger.info("Compute outlinks counter file")
            im.make_links_counter_file(CRAWL_ID, S3_URI, part_id, "out",
                                       tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)
            agg.compute_aggregators_from_part_id(CRAWL_ID, S3_URI, part_id,
                                                 tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)

        agg.consolidate_aggregators(CRAWL_ID, S3_URI,
                                    tmp_dir_prefix=TEST_DIR, force_fetch=force_fetch)