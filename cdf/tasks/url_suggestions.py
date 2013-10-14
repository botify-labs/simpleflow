import os
import gzip
import lz4

from cdf.streams.caster import Caster
from cdf.streams.utils import split_file
from cdf.collections.urls.generators.suggestions import UrlSuggestionsGenerator
from cdf.utils.s3 import fetch_file, push_content
from cdf.streams.mapping import STREAMS_HEADERS


def compute_urls_patterns_suggestions_from_s3(crawl_id, part_id, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    """
    Match all urls with suggested patterns coming for precomputed clusters

    Crawl dataset for this part_id is found by fetching all files finishing by .txt.[part_id] in the `s3_uri` called.

    :param part_id : the part_id from the crawl
    :param s3_uri : the location where the file will be pushed. filename will be url_properties.txt.[part_id]
    :param tmp_dir : the temporary directory where the S3 files will be put to compute the task
    :param force_fetch : fetch the S3 files even if they are already in the temp directory
    """
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(tmp_dir):
        try:
            os.makedirs(tmp_dir)
        except:
            pass

    patterns_file, _ = fetch_file(os.path.join(s3_uri, 'urlids.txt.%d.gz' % part_id), os.path.join(tmp_dir, 'urlids.txt.%d.gz' % part_id), force_fetch=force_fetch)
    cluster_path_file, _ = fetch_file(os.path.join(s3_uri, 'clusters_path.tsv'), os.path.join(tmp_dir, 'clusters_path.tsv'), force_fetch=force_fetch)
    cluster_qskey_file, _ = fetch_file(os.path.join(s3_uri, 'clusters_qskey.tsv'), os.path.join(tmp_dir, 'clusters_qskey.tsv'), force_fetch=force_fetch)

    cast = Caster(STREAMS_HEADERS["PATTERNS"]).cast
    stream_patterns = cast(split_file(gzip.open(patterns_file)))
    cluster_path_list = [k.split('\t', 1)[0] for k in open(cluster_path_file)]
    cluster_qskey_list = [k.split('\t', 1)[0] for k in open(cluster_qskey_file)]

    clusters = {
        'path': cluster_path_list,
        'qskey': cluster_qskey_list
    }

    content = []
    u = UrlSuggestionsGenerator(stream_patterns, clusters)
    for i, result in enumerate(u):
        content.append('{}\t{}\t{}'.format(result[0], result[1], result[2]))
    encoded_content = lz4.dumps('\n'.join(content))
    push_content(os.path.join(s3_uri, 'url_suggested_patterns.lz4'), encoded_content)
