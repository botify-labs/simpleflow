import os
import re

from autotagging.association_rules.algorithm import discover_host_patterns
from autotagging.association_rules.algorithm import discover_query_strings_patterns
from autotagging.association_rules.algorithm import discover_metadata_patterns
from autotagging.association_rules.algorithm import discover_path_patterns
from autotagging.association_rules.algorithm import discover_mixed_patterns
from autotagging.association_rules.algorithm import build_children_relationship
from autotagging.visualization.textual import save_apriori_algorithm_results
from autotagging.visualization.textual import save_child_relationship

from cdf.utils.path import makedirs
from cdf.utils.remote_files import nb_parts_from_crawl_location
from cdf.streams.mapping import CONTENT_TYPE_INDEX, CONTENT_TYPE_NAME_TO_ID
from cdf.collections.urls.constants import CLUSTER_TYPE_TO_ID
from cdf.log import logger
from cdf.utils.s3 import fetch_file, fetch_files, push_file
from cdf.streams.stream_factory import (PathStreamFactory,
                                        HostStreamFactory,
                                        QueryStringStreamFactory,
                                        MetadataStreamFactory,
                                        load_crawler_metakeys,
                                        get_nb_crawled_urls)


def compute_mixed_clusters(crawl_id,
                           s3_uri,
                           first_part_id_size,
                           part_id_size,
                           tmp_dir_prefix='/tmp',
                           force_fetch=False):

    minimal_frequency = 0.03

    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    makedirs(tmp_dir, exist_ok=True)

    output_dir = tmp_dir
    makedirs(output_dir, exist_ok=True)

    global_crawl_info_filename = "files.json"
    fetch_file(os.path.join(s3_uri, global_crawl_info_filename),
               os.path.join(tmp_dir, global_crawl_info_filename),
               force_fetch=force_fetch)

    for part_id in xrange(0, nb_parts_from_crawl_location(s3_uri)):
        fetch_files(s3_uri,
                    tmp_dir,
                    regexp=['url(ids|infos|contents).txt.%d.gz' % part_id],
                    force_fetch=force_fetch)

    logger.info("Compute patterns cluster")

    crawler_metakeys = load_crawler_metakeys(tmp_dir)
    nb_crawled_urls = get_nb_crawled_urls(tmp_dir)

    patterns = []

    host_stream_factory = HostStreamFactory(tmp_dir, crawler_metakeys)
    host_patterns = discover_host_patterns(host_stream_factory,
                                           minimal_frequency)

    #find patterns on hosts
    cluster_type = CLUSTER_TYPE_TO_ID["pattern"]["host"]
    patterns.append([(cluster_type, pattern, support) for pattern, support in host_patterns])

    #find patterns on pathes
    path_stream_factory = PathStreamFactory(tmp_dir, crawler_metakeys)
    path_patterns = discover_path_patterns(path_stream_factory,
                                           nb_crawled_urls,
                                           minimal_frequency)
    cluster_type = CLUSTER_TYPE_TO_ID["pattern"]["path"]
    patterns.append([(cluster_type, pattern, support) for pattern, support in path_patterns])

    query_string_stream_factory = QueryStringStreamFactory(tmp_dir,
                                                           crawler_metakeys)
    query_string_patterns = discover_query_strings_patterns(query_string_stream_factory,
                                                            minimal_frequency)
    cluster_type = CLUSTER_TYPE_TO_ID["pattern"]["qskey"]
    patterns.append([(cluster_type, pattern, support) for pattern, support in query_string_patterns])

    for metadata_type in ["title", "h1", "h2"]:
        logger.info("Discovering patterns on %s.", metadata_type)
        metadata_stream_factory = MetadataStreamFactory(tmp_dir,
                                                        metadata_type,
                                                        crawler_metakeys)
        metadata_patterns = discover_metadata_patterns(metadata_stream_factory,
                                                       nb_crawled_urls,
                                                       minimal_frequency)

        cluster_type = CLUSTER_TYPE_TO_ID["metadata"][CONTENT_TYPE_NAME_TO_ID[metadata_type]]
        patterns.append([(cluster_type, pattern, support) for pattern, support in metadata_patterns])

    mixed_patterns = discover_mixed_patterns(patterns, minimal_frequency)
    if output_dir:
        save_apriori_algorithm_results(mixed_patterns,
                                       output_dir,
                                       "mixed",
                                       first_part_id_size,
                                       part_id_size)
    push_file(
        os.path.join(s3_uri, 'clusters_mixed.tsv'),
        os.path.join(output_dir, 'clusters_mixed.tsv')
    )


    file_name_regex = re.compile('url_suggested_clusters.txt.\d+.gz')
    for file_name in os.listdir(tmp_dir):
        if not file_name_regex.match(file_name):
            continue
        push_file(
            os.path.join(s3_uri, file_name),
            os.path.join(tmp_dir, file_name),
            )

    children_dictionary = build_children_relationship(mixed_patterns)
    if output_dir:
        save_child_relationship(children_dictionary, output_dir)

    push_file(
        os.path.join(s3_uri, 'cluster_mixed_children.tsv'),
        os.path.join(output_dir, 'cluster_mixed_children.tsv')
    )
