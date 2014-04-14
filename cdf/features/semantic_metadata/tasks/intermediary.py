import os
import gzip
import itertools

from cdf.log import logger
from cdf.utils.path import write_by_part
from cdf.utils.s3 import fetch_files, push_file
from cdf.core.streams.caster import Caster
from cdf.metadata.raw import STREAMS_HEADERS, STREAMS_FILES
from cdf.analysis.urls.transducers.metadata_duplicate import get_duplicate_metadata
from cdf.core.streams.utils import split_file
from cdf.utils.remote_files import nb_parts_from_crawl_location
from .decorators import TemporaryDirTask as with_temporary_dir
from .constants import DEFAULT_FORCE_FETCH


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
