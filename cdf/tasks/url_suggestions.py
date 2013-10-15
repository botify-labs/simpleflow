import os
import gzip
import lz4

from cdf.streams.caster import Caster
from cdf.streams.utils import split_file
from cdf.collections.urls.generators.suggestions import UrlSuggestionsGenerator
from cdf.utils.s3 import fetch_file, fetch_files, push_content
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.urls.constants import SUGGEST_CLUSTERS
from cdf.log import logger


def compute_urls_suggestions_from_s3(crawl_id, part_id, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
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

    files_fetched = fetch_files(s3_uri,
                                tmp_dir,
                                regexp=['url(ids|infos|contents).txt.%d.gz' % part_id],
                                force_fetch=force_fetch)

    streams = dict()
    for path_local, fetched in files_fetched:
        stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
        cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
        streams[stream_identifier] = cast(split_file(gzip.open(path_local)))

    u = UrlSuggestionsGenerator(streams['patterns'], streams['infos'], streams['contents'])

    for cluster_type, cluster_name in SUGGEST_CLUSTERS:
        filename = 'clusters_{}_{}.tsv'.format(cluster_type, cluster_name)
        _f, fetched = fetch_file(os.path.join(s3_uri, filename), os.path.join(tmp_dir, filename), force_fetch=force_fetch)
        cluster_values = [k.split('\t', 1)[0] for k in open(_f)]
        if cluster_type == "metadata":
            u.add_metadata_cluster(cluster_name, cluster_values)
        else:
            u.add_pattern_cluster(cluster_name, cluster_values)

    content = []
    for i, result in enumerate(u):
        content.append('{}\t{}\t{}\t{}'.format(result[0], result[1], result[2], result[3]))
        if i % 1000 == 999:
            logger.info(content[-1])
    encoded_content = lz4.dumps('\n'.join(content))
    push_content(os.path.join(s3_uri, 'url_suggested_clusters.lz4'), encoded_content)
    push_content(os.path.join(s3_uri, 'url_suggested_clusters.txt'), '\n'.join(content))
