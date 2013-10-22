import os

from autotagging.association_rules.algorithm import discover_query_strings_patterns
from autotagging.association_rules.algorithm import discover_metadata_patterns
from autotagging.association_rules.algorithm import discover_path_patterns
from autotagging.loading.saas import Content_types
from autotagging.visualization.textual import save_apriori_algorithm_results

from cdf.log import logger
from cdf.utils.s3 import fetch_files, push_file


def compute_patterns_clusters(crawl_id, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    minimal_frequency = 0.03
    nb_urls = 100000

    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(tmp_dir):
        try:
            os.makedirs(tmp_dir)
        except:
            pass

    output_dir = os.path.join(tmp_dir, 'clusters')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # For now, compute clusters only with first part (to be improved)
    fetch_files(s3_uri,
                tmp_dir,
                regexp=['url(ids|contents).txt.0.gz'],
                force_fetch=force_fetch)

    logger.info("Compute patterns cluster")

    #find patterns on pathes
    path_patterns = discover_path_patterns(tmp_dir,
                                           nb_urls,
                                           minimal_frequency)
    if output_dir:
        save_apriori_algorithm_results(path_patterns,
                                       output_dir,
                                       "pattern_path")

    push_file(
        os.path.join(s3_uri, 'clusters_pattern_path.tsv'),
        os.path.join(output_dir, 'clusters_pattern_path.tsv')
    )

    query_string_patterns = discover_query_strings_patterns(tmp_dir,
                                                            nb_urls,
                                                            minimal_frequency)
    save_apriori_algorithm_results(query_string_patterns,
                                   output_dir,
                                   "pattern_qskey")
    push_file(
        os.path.join(s3_uri, 'clusters_pattern_qskey.tsv'),
        os.path.join(output_dir, 'clusters_pattern_qskey.tsv')
    )


def compute_metadata_clusters(crawl_id, s3_uri, tmp_dir_prefix='/tmp', force_fetch=False):
    minimal_frequency = 0.03
    nb_urls = 100000

    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)
    if not os.path.exists(tmp_dir):
        try:
            os.makedirs(tmp_dir)
        except:
            pass

    output_dir = os.path.join(tmp_dir, 'clusters')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for metadata_type in [Content_types.TITLE, Content_types.H1, Content_types.H2]:
        logger.info("Discovering patterns on %s.", Content_types.get_string(metadata_type))
        metadata_patterns = discover_metadata_patterns(tmp_dir,
                                                       nb_urls,
                                                       minimal_frequency,
                                                       metadata_type)
        suffix = Content_types.get_string(metadata_type)
        suffix = "%s_%s" % ("metadata", suffix)
        save_apriori_algorithm_results(metadata_patterns,
                                       output_dir,
                                       suffix)
        push_file(
            os.path.join(s3_uri, 'clusters_{}.tsv'.format(suffix)),
            os.path.join(output_dir, 'clusters_{}.tsv'.format(suffix))
        )
