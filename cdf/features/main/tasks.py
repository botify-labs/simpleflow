import os

import gzip

from autotagging.exceptions import TooManyCombinationsError
from autotagging.association_rules.algorithm import (discover_protocol_patterns,
                                                     discover_host_patterns,
                                                     discover_query_strings_patterns,
                                                     discover_metadata_patterns,
                                                     discover_path_patterns,
                                                     discover_mixed_patterns,
                                                     build_children_relationship)
from autotagging.visualization.textual import (save_mixed_clusters,
                                               save_url_suggested_clusters,
                                               save_child_relationship)

from cdf.utils.path import makedirs
from cdf.utils.remote_files import nb_parts_from_crawl_location
from cdf.features.semantic_metadata.settings import CONTENT_TYPE_NAME_TO_ID
from cdf.analysis.urls.constants import CLUSTER_TYPE_TO_ID
from cdf.log import logger
from cdf.utils.s3 import fetch_file, fetch_files, push_file
from cdf.features.main.streams import IdStreamDef, InfosStreamDef, ZoneStreamDef
from cdf.core.streams.utils import group_left
from cdf.core.streams.stream_factory import (ProtocolStreamFactory,
                                             PathStreamFactory,
                                             HostStreamFactory,
                                             QueryStringStreamFactory,
                                             MetadataStreamFactory,
                                             load_crawler_metakeys,
                                             get_nb_crawled_urls)
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir


@with_temporary_dir
def compute_suggested_patterns(crawl_id,
                               s3_uri,
                               first_part_id_size,
                               part_id_size,
                               tmp_dir=None,
                               force_fetch=False):
    minimal_frequency = 0.03

    # Fetch locally the files from S3
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

    ######################## protocol patterns ########################
    logger.info("Discovering patterns on protocol.")
    protocol_stream_factory = ProtocolStreamFactory(tmp_dir, crawler_metakeys)
    try:
        protocol_patterns = discover_protocol_patterns(protocol_stream_factory,
                                                       nb_crawled_urls,
                                                       minimal_frequency)

        cluster_type = 0  # the cluster_type is not used. TODO remove.
        patterns.append([(cluster_type, pattern, support) for
                         pattern, support in protocol_patterns])
    except TooManyCombinationsError as e:
        logger.warning("Could not compute patterns on protocols: '%s'.", str(e))

    ######################## host patterns ########################
    logger.info("Discovering patterns on host.")
    host_stream_factory = HostStreamFactory(tmp_dir, crawler_metakeys)
    try:
        host_patterns = discover_host_patterns(host_stream_factory,
                                               nb_crawled_urls,
                                               minimal_frequency)

        #find patterns on hosts
        cluster_type = CLUSTER_TYPE_TO_ID["pattern"]["host"]
        patterns.append([(cluster_type, pattern, support) for
                         pattern, support in host_patterns])
    except TooManyCombinationsError as e:
        logger.warning("Could not compute patterns on host: '%s'.", str(e))

    ######################## path patterns ###############################
    logger.info("Discovering patterns on path.")
    path_stream_factory = PathStreamFactory(tmp_dir, crawler_metakeys)
    try:
        path_patterns = discover_path_patterns(path_stream_factory,
                                               nb_crawled_urls,
                                               minimal_frequency)
        cluster_type = CLUSTER_TYPE_TO_ID["pattern"]["path"]
        patterns.append([(cluster_type, pattern, support) for
                         pattern, support in path_patterns])
    except TooManyCombinationsError as e:
        logger.warning("Could not compute patterns on path: '%s'.", str(e))

    ######################## query string patterns #######################
    logger.info("Discovering patterns on query string.")
    query_string_stream_factory = QueryStringStreamFactory(tmp_dir,
                                                           crawler_metakeys)
    try:
        query_string_patterns = discover_query_strings_patterns(query_string_stream_factory,
                                                                nb_crawled_urls,
                                                                minimal_frequency)
        cluster_type = CLUSTER_TYPE_TO_ID["pattern"]["qskey"]
        patterns.append([(cluster_type, pattern, support) for
                         pattern, support in query_string_patterns])
    except TooManyCombinationsError as e:
        logger.warning("Could not compute patterns on query string: '%s'.", str(e))

    ######################## metadata patterns ###########################
    for metadata_type in ["title", "description", "h1"]:
        logger.info("Discovering patterns on %s.", metadata_type)
        metadata_stream_factory = MetadataStreamFactory(tmp_dir,
                                                        metadata_type,
                                                        crawler_metakeys)
        try:
            metadata_patterns = discover_metadata_patterns(metadata_stream_factory,
                                                           nb_crawled_urls,
                                                           minimal_frequency)

            cluster_type = CLUSTER_TYPE_TO_ID["metadata"][CONTENT_TYPE_NAME_TO_ID[metadata_type]]
            patterns.append([(cluster_type, pattern, support) for
                             pattern, support in metadata_patterns])
        except TooManyCombinationsError as e:
            logger.warning("Could not compute patterns on %s: '%s'",
                           metadata_type, str(e))

    logger.info("Mixing patterns from different kinds of data together.")
    mixed_patterns = discover_mixed_patterns(patterns,
                                             nb_crawled_urls,
                                             minimal_frequency)

    ######################## save results ################################
    files_to_push = []

    mixed_clusters_filepath = save_mixed_clusters(mixed_patterns,
                                                  output_dir,
                                                  "mixed")
    files_to_push.append(mixed_clusters_filepath)

    suggested_clusters_files = save_url_suggested_clusters(mixed_patterns,
                                                           output_dir,
                                                           first_part_id_size,
                                                           part_id_size)
    files_to_push.extend(suggested_clusters_files)

    logger.info("Computing children relationship between patterns.")
    children_dictionary = build_children_relationship(mixed_patterns)
    children_filepath = save_child_relationship(children_dictionary,
                                                output_dir)
    files_to_push.append(children_filepath)

    #push files to s3
    for filepath in files_to_push:
        push_file(
            os.path.join(s3_uri, os.path.basename(filepath)),
            os.path.join(filepath)
        )


@with_temporary_dir
def compute_zones(crawl_id,
                  s3_uri,
                  part_id,
                  tmp_dir=None,
                  force_fetch=False):
    #get base streams
    id_stream = IdStreamDef.get_stream_from_s3(s3_uri,
                                               tmp_dir=tmp_dir,
                                               part_id=part_id)
    info_stream = InfosStreamDef.get_stream_from_s3(s3_uri,
                                                    tmp_dir=tmp_dir,
                                                    part_id=part_id)
    #group streams
    group_stream = group_left((id_stream, 0), info=(info_stream, 0))
    protocol_idx = IdStreamDef.field_idx("protocol")
    lang_idx = InfosStreamDef.field_idx("lang")

    output_file_name = "{}.txt.{}.gz".format(ZoneStreamDef.FILE, part_id)
    output_file_path = os.path.join(tmp_dir, output_file_name)
    with gzip.open(output_file_path, "w") as f:
        for urlid, id_entry, info_entry in group_stream:
            protocol = id_entry[protocol_idx]
            lang = info_entry["info"][0][lang_idx]
            f.write("{}\t{}\n".format(urlid, "{},{}".format(lang, protocol)))

    #push file to s3
    s3_destination = "{}/{}".format(s3_uri, output_file_name)
    push_file(
        s3_destination,
        output_file_path
    )

    return s3_destination
