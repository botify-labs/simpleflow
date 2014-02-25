import os
import gzip
import itertools

from boto.exception import S3ResponseError

from cdf.log import logger
from cdf.utils.path import write_by_part
from cdf.utils.s3 import fetch_files, fetch_file, push_file
from cdf.streams.caster import Caster
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.urls.transducers.metadata_duplicate import get_duplicate_metadata
from cdf.collections.urls.transducers.links import OutlinksTransducer, InlinksTransducer
from cdf.streams.utils import split_file
from cdf.utils.remote_files import nb_parts_from_crawl_location
from cdf.collections.urls.generators.bad_links import get_bad_links, get_bad_link_counters
from .decorators import TemporaryDirTask as with_temporary_dir
from .constants import DEFAULT_FORCE_FETCH


@with_temporary_dir
def make_links_counter_file(crawl_id, s3_uri,
                            part_id, link_direction,
                            tmp_dir=None, force_fetch=DEFAULT_FORCE_FETCH):
    if link_direction == "out":
        transducer = OutlinksTransducer
        stream_name = "outlinks_raw"
    else:
        transducer = InlinksTransducer
        stream_name = "inlinks_raw"

    logger.info('Fetching files from s3 for part {}'.format(part_id))
    links_file_path = 'url{}.txt.{}.gz'.format(
        "links" if link_direction == "out" else "inlinks", part_id)
    try:
        links_file, fecthed = fetch_file(
            os.path.join(s3_uri, links_file_path),
            os.path.join(tmp_dir, links_file_path),
            force_fetch=force_fetch
        )
    except S3ResponseError as e:
        logger.error(str(e))
        return

    cast = Caster(STREAMS_HEADERS[stream_name.upper()]).cast
    stream_links = cast(split_file(gzip.open(links_file)))
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
def make_metadata_duplicates_file(crawl_id, s3_uri,
                                  first_part_id_size, part_id_size,
                                  tmp_dir=None, force_fetch=DEFAULT_FORCE_FETCH):
    def to_string(row):
        url_id, metadata_type, filled_nb, duplicates_nb, is_first, target_urls_ids = row
        return '\t'.join((
            str(url_id),
            str(metadata_type),
            str(filled_nb),
            str(duplicates_nb),
            '1' if is_first else '0',
            ';'.join(str(u) for u in target_urls_ids)
        )) + '\n'

    streams_types = {'contents': []}
    nb_parts = nb_parts_from_crawl_location(s3_uri)

    logger.info('Fetching all partitions from S3')
    for part_id in xrange(0, nb_parts):
        files_fetched = fetch_files(s3_uri,
                                    tmp_dir,
                                    regexp='urlcontents.txt.%d.gz' % part_id,
                                    force_fetch=force_fetch)

        for path_local, fetched in files_fetched:
            stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
            cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
            streams_types[stream_identifier].append(cast(split_file(gzip.open(path_local))))

    if len(streams_types['contents']) == 0:
        logger.warn("Could not fetch any urlcontents file.")
        return

    generator = get_duplicate_metadata(itertools.chain(*streams_types['contents']))

    file_pattern = 'urlcontentsduplicate.txt.{}.gz'
    write_by_part(generator, first_part_id_size, part_id_size,
                  tmp_dir, file_pattern, to_string)

    # push all created files to s3
    logger.info('Pushing metadata duplicate file to s3')
    for i in xrange(0, nb_parts + 1):
        file_to_push = file_pattern.format(i)
        if os.path.exists(os.path.join(tmp_dir, file_to_push)):
            logger.info('Pushing {}'.format(file_to_push))
            push_file(
                os.path.join(s3_uri, file_to_push),
                os.path.join(tmp_dir, file_to_push),
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
    def to_string(row):
        return '\t'.join(str(field) for field in row) + '\n'

    streams_types = {'infos': [],
                     'outlinks': []}
    nb_parts = nb_parts_from_crawl_location(s3_uri)

    logger.info('Fetching all partition info and links files from s3')
    for part_id in xrange(0, nb_parts):
        files_fetched = fetch_files(s3_uri,
                                    tmp_dir,
                                    regexp='url(infos|links).txt.%d.gz' % part_id,
                                    force_fetch=force_fetch)

        for path_local, fetched in files_fetched:
            stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
            cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
            streams_types[stream_identifier].append(cast(split_file(gzip.open(path_local))))

    generator = get_bad_links(itertools.chain(*streams_types['infos']),
                              itertools.chain(*streams_types['outlinks']))

    file_pattern = 'urlbadlinks.txt.{}.gz'
    write_by_part(generator, first_part_id_size, part_id_size,
                  tmp_dir, file_pattern, to_string)

    # push all created files to s3
    logger.info('Pushing badlink files to s3')
    for i in xrange(0, nb_parts + 1):
        file_to_push = file_pattern.format(i)
        if os.path.exists(os.path.join(tmp_dir, file_to_push)):
            logger.info('Pushing {}'.format(file_to_push))
            push_file(
                os.path.join(s3_uri, file_to_push),
                os.path.join(tmp_dir, file_to_push),
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
    file_name = 'urlbadlinks.txt.%d.gz' % part_id
    logger.info('Fetching file from s3 for part {}'.format(part_id))
    try:
        bad_link_file, _ = fetch_file(os.path.join(s3_uri, file_name),
                                      os.path.join(tmp_dir, file_name),
                                      force_fetch=force_fetch)
    except S3ResponseError:
        logger.info("{} is not found from s3".format(file_name))
        return

    streams_types = {'badlinks': []}

    stream_identifier = STREAMS_FILES[os.path.basename(bad_link_file).split('.')[0]]
    cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
    streams_types[stream_identifier].append(cast(split_file(gzip.open(bad_link_file))))

    generator = get_bad_link_counters(itertools.chain(*streams_types['badlinks']))

    file_name = 'urlbadlinks_counters.txt.%d.gz' % part_id
    f = gzip.open(os.path.join(tmp_dir, file_name), 'w')
    for src, bad_code, count in generator:
        f.write('\t'.join((
            str(src),
            str(bad_code),
            str(count)
        )) + '\n')
    f.close()

    logger.info('Pushing {}'.format(file_name))
    push_file(
        os.path.join(s3_uri, file_name),
        os.path.join(tmp_dir, file_name),
    )
