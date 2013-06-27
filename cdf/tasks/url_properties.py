import os
import gzip
import lz4

from cdf.streams.constants import STREAMS_HEADERS
from cdf.streams.caster import Caster
from cdf.streams.utils import split_file
from cdf.collections.url_properties.generator import UrlPropertiesGenerator
from cdf.utils.s3 import fetch_files, push_content


def compute_properties_from_s3(crawl_id, part_id, rev_num, s3_uri, settings, tmp_dir_prefix='/tmp', force_fetch=False):
    """
    Match all urls from a crawl's `part_id` to properties defined by rules in a `settings` dictionnary and save it to a S3 bucket.

    Crawl dataset for this part_id is found by fetching all files finishing by .txt.[part_id] in the `s3_uri` called.

    :param part_id : the part_id from the crawl
    :param s3_uri : the location where the file will be pushed. filename will be url_properties.txt.[part_id]
    :param tmp_dir : the temporary directory where the S3 files will be put to compute the task
    :param force_fetch : fetch the S3 files even if they are already in the temp directory
    """

    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    files_fetched = fetch_files(s3_uri, tmp_dir, prefixes=('urlid', ), suffixes=('.txt.%d.gz' % part_id, ), force_fetch=force_fetch)
    path_local, fetched = files_fetched[0]

    cast = Caster(STREAMS_HEADERS['PATTERNS']).cast
    stream_patterns = cast(split_file(gzip.open(path_local)))

    g = UrlPropertiesGenerator(stream_patterns, settings)

    map_func = lambda k: '\t'.join((str(k[0]), k[1]['resource_type']))
    content = '\n'.join(map(map_func, g))

    encoded_content = lz4.dumps(content)
    push_content(os.path.join(s3_uri, 'url_properties_rev%d.txt.lz4.%d' % (rev_num, part_id)), encoded_content)
