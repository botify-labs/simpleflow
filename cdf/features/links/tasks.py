import gzip
import os
import itertools
import logging

from cdf.compat import json
from cdf.utils.s3 import push_file, push_content
from cdf.core.constants import FIRST_PART_ID_SIZE, PART_ID_SIZE
from cdf.utils.remote_files import (
    get_crawl_info,
    get_max_crawled_urlid,
)
from cdf.features.links.links import OutlinksTransducer, InlinksTransducer
from cdf.features.links.bad_links import (
    get_bad_links,
    get_bad_link_counters,
    get_links_to_non_compliant_urls,
    get_link_to_non_compliant_urls_counters
)
from cdf.features.main.streams import (
    InfosStreamDef,
    CompliantUrlStreamDef,
    IdStreamDef
)
from cdf.features.links.streams import (
    OutlinksRawStreamDef, OutlinksStreamDef,
    InlinksRawStreamDef,
    InlinksCountersStreamDef,
    BadLinksStreamDef,
    BadLinksCountersStreamDef,
    LinksToNonCompliantStreamDef,
    LinksToNonCompliantCountersStreamDef,
    InlinksPercentilesStreamDef,
    InredirectCountersStreamDef
)
from cdf.features.links.top_domains import (
    compute_top_domain,
    filter_external_outlinks,
    filter_invalid_destination_urls,
    resolve_sample_url_id,
    remove_unused_columns,
)
from cdf.features.links.percentiles import (
    compute_quantiles,
    compute_percentile_stats
)
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir
from cdf.tasks.constants import DEFAULT_FORCE_FETCH


logger = logging.getLogger(__name__)


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

    # if this task has generated some files (it operates on existing partition)
    # returns True, else False
    if len(file_created) > 0:
        return part_id
    return None


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
def make_links_to_non_compliant_file(s3_uri,
                                     first_part_id_size,
                                     part_id_size,
                                     tmp_dir=None, force_fetch=DEFAULT_FORCE_FETCH):
    """
    Generate a tsv file that list all urls outlink to a non compliant url:
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

    generator = get_links_to_non_compliant_urls(
        CompliantUrlStreamDef.load(**stream_kwargs),
        OutlinksStreamDef.load(**stream_kwargs)
    )

    LinksToNonCompliantStreamDef.persist(
        generator, s3_uri,
        first_part_size=first_part_id_size,
        part_size=part_id_size
    )


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
def make_links_to_non_compliant_counter_file(s3_uri,
                                             part_id,
                                             tmp_dir=None,
                                             force_fetch=DEFAULT_FORCE_FETCH):
    """
    Generate a counter file that list link to non compliant url counts by source url
      url_src_id  count

    This method depend on the file generated by `make_links_to_non_compliant_file`
    Ordered on url_src_id and http_code
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param part_id: the part id to process
    :type part_id: int
    :returns: list - the list of generated files
    """
    stream = LinksToNonCompliantStreamDef.load(
        s3_uri,
        tmp_dir=tmp_dir,
        part_id=part_id,
        force_fetch=force_fetch
    )
    generator = get_link_to_non_compliant_urls_counters(stream)
    LinksToNonCompliantCountersStreamDef.persist(
        generator,
        s3_uri,
        part_id=part_id
    )


@with_temporary_dir
def make_top_domains_files(crawl_id,
                           s3_uri,
                           nb_top_domains,
                           crawled_partitions,
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
    :param crawled_partitions: crawled partition ids
    :type crawled_partitions: list
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
    outlinks = OutlinksRawStreamDef.load(
        s3_uri, tmp_dir=tmp_dir, force_fetch=force_fetch)
    outlinks = filter_external_outlinks(outlinks)
    outlinks = filter_invalid_destination_urls(outlinks)
    outlinks = remove_unused_columns(outlinks)

    streams = [
        IdStreamDef.load(
            s3_uri, part_id=i, tmp_dir=tmp_dir, force_fetch=force_fetch)
        for i in crawled_partitions
    ]
    urlids_stream = itertools.chain(*streams)

    logger.info("Computing top domains")
    tld_result, sld_result = compute_top_domain(
        outlinks, nb_top_domains, tmp_dir)

    # resolve urlids
    logger.info("Resolve urlids")
    resolve_sample_url_id(urlids_stream, tld_result + sld_result)

    # persist results
    logger.info("Persist results")
    tld_destination = os.path.join(s3_uri, 'top_full_domains.json')
    push_content(
        tld_destination,
        json.dumps([domain.to_dict() for domain in tld_result])
    )
    sld_destination = os.path.join(s3_uri, 'top_second_level_domains.json')
    push_content(
        sld_destination,
        json.dumps([domain.to_dict() for domain in sld_result])
    )

    result = [tld_destination, sld_destination]
    return result


@with_temporary_dir
def make_inlinks_percentiles_file(s3_uri,
                                  first_part_id_size=FIRST_PART_ID_SIZE,
                                  part_id_size=PART_ID_SIZE,
                                  tmp_dir=None,
                                  force_fetch=DEFAULT_FORCE_FETCH):
    """Compute the InlinksPercentilesStreamDef stream that assigns
    a percentile id to every crawled url

    Also compute the percentile graph data

    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param first_part_id_size: the size of the first part.
    :type first_part_id_size: int
    :param part_id_size: the size of the parts (except the first one)
    :type part_id_size: int
    :returns: list - the list of generated files
    """

    #get streams
    urlid_stream = InfosStreamDef.load(s3_uri, tmp_dir=tmp_dir)
    inlinks_counter_stream = InlinksCountersStreamDef.load(s3_uri, tmp_dir=tmp_dir)
    inredirections_counter_stream = InredirectCountersStreamDef.load(s3_uri, tmp_dir=tmp_dir)
    #get max crawled urlid
    crawler_metakeys = get_crawl_info(s3_uri, tmp_dir=tmp_dir)
    max_crawled_urlid = get_max_crawled_urlid(crawler_metakeys)
    #generate stream
    nb_quantiles = 100
    percentile_stream = compute_quantiles(
        urlid_stream,
        inlinks_counter_stream,
        inredirections_counter_stream,
        max_crawled_urlid,
        nb_quantiles
    )
    #store percentile stream in memory
    #3 ints, at most 1M elements
    #so at most 20MB
    percentile_stream = list(percentile_stream)

    #compute inlink percentile graph data
    stats = compute_percentile_stats(percentile_stream)
    dest_uri = os.path.join(
        s3_uri, 'precomputation', 'inlinks_percentiles.json')

    result = {
        'domain': 'inlinks',
        'percentiles': [s.to_dict() for s in stats]
    }
    push_content(dest_uri, json.dumps(result))

    #persist stream
    InlinksPercentilesStreamDef.persist(
        percentile_stream,
        s3_uri,
        first_part_size=first_part_id_size,
        part_size=part_id_size
    )