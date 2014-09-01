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
from cdf.features.links.streams import OutlinksStreamDef
from cdf.features.main.strategic_url import generate_strategic_stream

from cdf.utils.path import makedirs
from cdf.utils.remote_files import nb_parts_from_crawl_location
from cdf.features.semantic_metadata.settings import CONTENT_TYPE_NAME_TO_ID
from cdf.analysis.urls.constants import CLUSTER_TYPE_TO_ID
from cdf.log import logger
from cdf.utils.s3 import fetch_file, fetch_files, push_file
from cdf.core.streams.stream_factory import (StreamFactoryCache,
                                             FileStreamFactory,
                                             ProtocolStreamFactory,
                                             PathStreamFactory,
                                             HostStreamFactory,
                                             QueryStringStreamFactory,
                                             MetadataStreamFactory,
                                             load_crawler_metakeys,
                                             get_nb_crawled_urls)
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir
from cdf.features.main.streams import (
    IdStreamDef,
    InfosStreamDef,
    ZoneStreamDef,
    StrategicUrlStreamDef
)
from cdf.features.main.zones import generate_zone_stream


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
    urlids_stream_factory = FileStreamFactory(tmp_dir,
                                              "urlids",
                                              crawler_metakeys)
    #use stream caching on file stream factory to improve performance
    urlids_stream_factory = StreamFactoryCache(
        urlids_stream_factory,
        tmp_dir
    )

    urlcontents_stream_factory = FileStreamFactory(tmp_dir,
                                                   "urlcontents",
                                                   crawler_metakeys)
    #use stream caching on file stream factory to improve performance
    urlcontents_stream_factory = StreamFactoryCache(
        urlcontents_stream_factory,
        tmp_dir
    )
    ######################## protocol patterns ########################
    logger.info("Discovering patterns on protocol.")
    protocol_stream_factory = ProtocolStreamFactory(urlids_stream_factory, crawler_metakeys)
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
    host_stream_factory = HostStreamFactory(urlids_stream_factory, crawler_metakeys)
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
    path_stream_factory = PathStreamFactory(urlids_stream_factory, crawler_metakeys)
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
    query_string_stream_factory = QueryStringStreamFactory(urlids_stream_factory,
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
        metadata_stream_factory = MetadataStreamFactory(urlcontents_stream_factory,
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
def compute_zones(s3_uri,
                  part_id,
                  tmp_dir=None,
                  force_fetch=False):
    """A task to compute the zones for a given part
    :param s3_uri: the uri where the crawl data is stored
    :type s3_uri: str
    :param part_id: the id of the part to process
    :type part_id:int
    :param tmp_dir: the path to the tmp directory to use.
                    If None, a new tmp directory will be created.
    :param tmp_dir: str
    :param force_fetch: if True, the files will be downloaded from s3
                        even if they are in the tmp directory.
                        if False, files that are present in the tmp_directory
                        will not be downloaded from s3.
    :rtype: string - the s3_uri of the generated zone file
    """
    #get base streams
    id_stream = IdStreamDef.get_stream_from_s3(s3_uri,
                                               tmp_dir=tmp_dir,
                                               part_id=part_id)
    info_stream = InfosStreamDef.get_stream_from_s3(s3_uri,
                                                    tmp_dir=tmp_dir,
                                                    part_id=part_id)
    s3_destination = ZoneStreamDef.persist_part_to_s3(
        generate_zone_stream(id_stream, info_stream),
        s3_uri,
        part_id
    )
    return s3_destination


@with_temporary_dir
def compute_strategic_urls(crawl_id, s3_uri, part_id,
                           tmp_dir=None, force_fetch=False):
    # prepare streams
    infos_stream = InfosStreamDef.get_stream_from_s3(
        s3_uri, tmp_dir, part_id=part_id,
        force_fetch=force_fetch
    )
    outlinks_stream = OutlinksStreamDef.get_stream_from_s3(
        s3_uri, tmp_dir, part_id=part_id,
        force_fetch=force_fetch
    )

    stream = generate_strategic_stream(infos_stream, outlinks_stream)
    StrategicUrlStreamDef.persist_part_to_s3(
        stream=stream,
        s3_uri=s3_uri,
        part_id=part_id
    )
