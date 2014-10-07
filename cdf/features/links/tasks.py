import os
import gzip
import json

from cdf.log import logger
from cdf.core.streams.cache import BufferedMarshalStreamCache
from cdf.utils.es import EsHandler
from cdf.utils.s3 import push_file, push_content, fetch_file
from cdf.core.constants import FIRST_PART_ID_SIZE, PART_ID_SIZE
from cdf.core.streams.stream_factory import (
    load_crawler_metakeys,
    get_max_crawled_urlid
)
from cdf.features.links.links import OutlinksTransducer, InlinksTransducer
from cdf.features.links.bad_links import (
    get_bad_links,
    get_bad_link_counters,
    get_links_to_non_strategic_urls,
    get_link_to_non_strategic_urls_counters
)
from cdf.features.main.streams import (
    InfosStreamDef,
    StrategicUrlStreamDef,
    IdStreamDef
)
from cdf.features.links.streams import (
    OutlinksRawStreamDef, OutlinksStreamDef,
    InlinksRawStreamDef,
    InlinksCountersStreamDef,
    BadLinksStreamDef,
    BadLinksCountersStreamDef,
    LinksToNonStrategicStreamDef,
    LinksToNonStrategicCountersStreamDef,
    InlinksPercentilesStreamDef
)
from cdf.features.links.top_domains import (
    compute_top_full_domains,
    compute_top_second_level_domains,
    filter_external_outlinks,
    filter_invalid_destination_urls,
    resolve_sample_url_id
)
from cdf.features.links.percentiles import compute_quantiles
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir
from cdf.tasks.constants import DEFAULT_FORCE_FETCH


@with_temporary_dir
def make_links_counter_file(crawl_id, s3_uri,
                            part_id, link_direction,
                            tmp_dir=None, force_fetch=DEFAULT_FORCE_FETCH):
    if link_direction == "out":
        transducer = OutlinksTransducer
        stream_name = OutlinksRawStreamDef
    else:
        transducer = InlinksTransducer
        stream_name = InlinksRawStreamDef

    stream_links = stream_name().load(s3_uri, tmp_dir, part_id, force_fetch)
    generator = transducer(stream_links).get()

    filenames = {
        'links': 'url_{}_links_counters.txt.{}.gz'.format(link_direction, part_id),
        'canonical': 'url_{}_canonical_counters.txt.{}.gz'.format(link_direction, part_id),
        'redirect': 'url_{}_redirect_counters.txt.{}.gz'.format(link_direction, part_id),
    }

    # lazily open files
    file_created = {}
    for i, entry in enumerate(generator):
        # TODO remove hard coded index
        link_type = entry[1]
        if link_type not in file_created:
            file_created[link_type] = gzip.open(os.path.join(tmp_dir, filenames[link_type]), 'w')
        file_created[link_type].write(str(entry[0]) + '\t' + '\t'.join(str(k) for k in entry[2:]) + '\n')

    for _f in file_created.itervalues():
        _f.close()

    # push all created files to s3
    logger.info('Pushing links counter files to S3')
    for counter_file in file_created.values():
        counter_filename = os.path.basename(counter_file.name)
        logger.info('Pushing {}'.format(counter_filename))
        push_file(
            os.path.join(s3_uri, counter_filename),
            os.path.join(tmp_dir, counter_filename),
        )


@with_temporary_dir
def make_bad_link_file(crawl_id, s3_uri,
                       first_part_id_size=500000,
                       part_id_size=500000,
                       tmp_dir=None, force_fetch=DEFAULT_FORCE_FETCH):
    """
    Generate a tsv file that list all urls outlink to an error url:
      url_src_id  url_dest_id error_http_code

    Ordered on url_src_id
    """
    stream_kwargs = {
        'uri': s3_uri,
        'tmp_dir': tmp_dir,
        'force_fetch': force_fetch,
    }

    generator = get_bad_links(
        InfosStreamDef.load(**stream_kwargs),
        OutlinksStreamDef.load(**stream_kwargs)
    )

    BadLinksStreamDef.persist(
        generator, s3_uri,
        first_part_size=first_part_id_size,
        part_size=part_id_size
    )


@with_temporary_dir
def make_links_to_non_strategic_file(s3_uri,
                                     first_part_id_size,
                                     part_id_size,
                                     tmp_dir=None, force_fetch=DEFAULT_FORCE_FETCH):
    """
    Generate a tsv file that list all urls outlink to a non strategic url:
      url_src_id  url_dest_id

    Ordered on url_src_id
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param first_part_id_size: the size of the first part
    :type first_part_id_size: int
    :param part_id_size: the size of the parts
    :type part_id_size: int
    :returns: list - the list of generated files
    """
    stream_kwargs = {
        'uri': s3_uri,
        'tmp_dir': tmp_dir,
        'force_fetch': force_fetch,
    }

    generator = get_links_to_non_strategic_urls(
        StrategicUrlStreamDef.load(**stream_kwargs),
        OutlinksStreamDef.load(**stream_kwargs)
    )

    result = LinksToNonStrategicStreamDef.persist(
        generator, s3_uri,
        first_part_size=first_part_id_size,
        part_size=part_id_size
    )
    return result

@with_temporary_dir
def make_bad_link_counter_file(crawl_id, s3_uri,
                               part_id,
                               tmp_dir=None,
                               force_fetch=DEFAULT_FORCE_FETCH):
    """
    Generate a counter file that list bad link counts by source url and http code
      url_src_id  http_code  count

    This method depend on the file generated by `make_bad_link_file`
    Ordered on url_src_id and http_code
    """
    stream = BadLinksStreamDef.load(
        s3_uri,
        tmp_dir=tmp_dir,
        part_id=part_id,
        force_fetch=force_fetch
    )
    generator = get_bad_link_counters(stream)
    BadLinksCountersStreamDef.persist(
        generator,
        s3_uri,
        part_id=part_id
    )


@with_temporary_dir
def make_links_to_non_strategic_counter_file(s3_uri,
                                             part_id,
                                             tmp_dir=None,
                                             force_fetch=DEFAULT_FORCE_FETCH):
    """
    Generate a counter file that list link to non strategic url counts by source url
      url_src_id  count

    This method depend on the file generated by `make_links_to_non_strategic_file`
    Ordered on url_src_id and http_code
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param part_id: the part id to process
    :type part_id: int
    :returns: list - the list of generated files
    """
    stream = LinksToNonStrategicStreamDef.load(
        s3_uri,
        tmp_dir=tmp_dir,
        part_id=part_id,
        force_fetch=force_fetch
    )
    generator = get_link_to_non_strategic_urls_counters(stream)
    result = LinksToNonStrategicCountersStreamDef.persist(
        generator,
        s3_uri,
        part_id=part_id
    )

    return result


@with_temporary_dir
def make_top_domains_files(crawl_id,
                           s3_uri,
                           nb_top_domains,
                           es_location,
                           es_index,
                           es_doc_type,
                           tmp_dir=None,
                           force_fetch=DEFAULT_FORCE_FETCH):
    """Compute top domains and top second level domains for a given crawl.
    :param crawl_id: crawl id
    :type crawl_id: int
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param nb_top_domains: the number of top domains to return
                           (typical value: 100)
    :type nb_top_domains: int
    :param es_location:
    :param es_index:
    :param es_doc_type:
    :param tmp_dir: the path to the tmp directory to use.
                    If None, a new tmp directory will be created.
    :type tmp_dir: str
    :param force_fetch: if True, the files will be downloaded from s3
                        even if they are in the tmp directory.
                        if False, files that are present in the tmp_directory
                        will not be downloaded from s3.
    :type force_fetch: bool
    :returns: the list of s3_uri for the generated files.
    :rtype: list
    """
    logger.info("Preprocessing and caching stream.")
    es_handler = EsHandler(es_location, es_index, es_doc_type)
    outlinks = OutlinksRawStreamDef.load(s3_uri, tmp_dir=tmp_dir)
    outlinks = filter_external_outlinks(outlinks)
    outlinks = filter_invalid_destination_urls(outlinks)

    stream_cache = BufferedMarshalStreamCache()
    stream_cache.cache(outlinks)

    result = []

    logger.info("Computing top %d full domains.", nb_top_domains)
    top_domains = compute_top_full_domains(
        stream_cache.get_stream(),
        nb_top_domains
    )
    # resolve url ids
    resolve_sample_url_id(es_handler, crawl_id, top_domains)
    s3_destination = "{}/top_full_domains.json".format(s3_uri)
    push_content(
        s3_destination,
        json.dumps([domain.to_dict() for domain in top_domains])
    )
    result.append(s3_destination)

    logger.info("Computing top %d second level domains.", nb_top_domains)
    top_domains = compute_top_second_level_domains(
        stream_cache.get_stream(),
        nb_top_domains
    )
    # resolve url ids
    resolve_sample_url_id(es_handler, crawl_id, top_domains)
    s3_destination = "{}/top_second_level_domains.json".format(s3_uri)
    push_content(
        s3_destination,
        json.dumps([domain.to_dict() for domain in top_domains])
    )
    result.append(s3_destination)

    return result


@with_temporary_dir
def make_inlinks_percentiles_file(s3_uri,
                                  first_part_id_size=FIRST_PART_ID_SIZE,
                                  part_id_size=PART_ID_SIZE,
                                  tmp_dir=None,
                                  force_fetch=DEFAULT_FORCE_FETCH):
    """Compute the InlinksPercentilesStreamDef stream that assigns
    a percentile id to every crawled url.
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param first_part_id_size: the size of the first part.
    :type first_part_id_size: int
    :param part_id_size: the size of the parts (except the first one)
    :type part_id_size: int
    :returns: list - the list of generated files
    """

    #get streams
    urlid_stream = IdStreamDef.load(s3_uri, tmp_dir=tmp_dir)
    inlinks_counter_stream = InlinksCountersStreamDef.load(s3_uri, tmp_dir=tmp_dir)

    #get max crawled urlid
    global_crawl_info_filename = "files.json"
    fetch_file(os.path.join(s3_uri, global_crawl_info_filename),
               os.path.join(tmp_dir, global_crawl_info_filename),
               force_fetch=force_fetch)
    crawler_metakeys = load_crawler_metakeys(tmp_dir)
    max_crawled_urlid = get_max_crawled_urlid(crawler_metakeys)
    #generate stream
    nb_quantiles = 100
    percentile_stream = compute_quantiles(urlid_stream,
                                          inlinks_counter_stream,
                                          max_crawled_urlid,
                                          nb_quantiles)
    #persist stream
    output_files = InlinksPercentilesStreamDef.persist(
        percentile_stream,
        s3_uri,
        first_part_size=first_part_id_size,
        part_size=part_id_size
    )
    return output_files
