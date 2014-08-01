import os
import itertools

from cdf.log import logger
from cdf.utils.path import write_by_part
from cdf.utils.s3 import push_file
from cdf.analysis.urls.transducers.metadata_duplicate import get_duplicate_metadata
from cdf.utils.remote_files import nb_parts_from_crawl_location
from cdf.features.semantic_metadata.streams import ContentsStreamDef
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir
from cdf.tasks.constants import DEFAULT_FORCE_FETCH


@with_temporary_dir
def make_metadata_duplicates_file(crawl_id, s3_uri,
                                  first_part_id_size, part_id_size,
                                  tmp_dir=None, force_fetch=DEFAULT_FORCE_FETCH, *args, **kwargs):
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

    nb_parts = nb_parts_from_crawl_location(s3_uri)

    logger.info('Fetching all partitions from S3')
    streams = []
    for part_id in xrange(0, nb_parts):
        streams.append(
            ContentsStreamDef.get_stream_from_s3(
                s3_uri,
                tmp_dir=tmp_dir,
                part_id=part_id,
                force_fetch=force_fetch
            )
        )

    generator = get_duplicate_metadata(itertools.chain(*streams))

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
