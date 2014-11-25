import itertools
from cdf.log import logger
from cdf.features.semantic_metadata.metadata_duplicate import (
    get_duplicate_metadata,
    get_context_aware_duplicate_metadata,
    count_metadata
)
from cdf.features.semantic_metadata.streams import (
    ContentsStreamDef,
    ContentsDuplicateStreamDef,
    ContentsContextAwareDuplicateStreamDef,
    ContentsCountStreamDef
)
from cdf.features.main.streams import (
    ZoneStreamDef,
    CompliantUrlStreamDef
)
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir
from cdf.tasks.constants import DEFAULT_FORCE_FETCH


@with_temporary_dir
def compute_metadata_count(s3_uri, part_id, tmp_dir=None):
    contents_stream = ContentsStreamDef.load(
        s3_uri,
        part_id=part_id,
        tmp_dir=tmp_dir
    )
    output_stream = count_metadata(contents_stream, part_id)

    s3_destination = ContentsCountStreamDef.persist(
        output_stream,
        s3_uri,
        part_id=part_id
    )

    return s3_destination


def to_string(row):
    """Format a row from get_duplicate_metadata() so that
    it can be processed by persist()
    It transforms the booleans in integer
    and the lists in strings.
    """
    url_id, metadata_type, duplicates_nb, is_first, target_urls_ids = row
    return (url_id,
            metadata_type,
            duplicates_nb,
            1 if is_first else 0,
            ';'.join(str(u) for u in target_urls_ids)
           )


@with_temporary_dir
def make_metadata_duplicates_file(crawl_id, s3_uri,
                                  first_part_id_size, part_id_size,
                                  tmp_dir=None,
                                  force_fetch=DEFAULT_FORCE_FETCH):
    logger.info('Fetching contents stream from S3')
    contents_stream = ContentsStreamDef.load(
        s3_uri,
        tmp_dir=tmp_dir,
        force_fetch=force_fetch
    )
    generator = get_duplicate_metadata(contents_stream)
    generator = itertools.imap(to_string, generator)
    files = ContentsDuplicateStreamDef.persist(
        generator,
        s3_uri,
        first_part_size=first_part_id_size,
        part_size=part_id_size
    )
    return files


@with_temporary_dir
def make_context_aware_metadata_duplicates_file(s3_uri,
                                             first_part_id_size,
                                             part_id_size,
                                             tmp_dir=None,
                                             force_fetch=DEFAULT_FORCE_FETCH):
    """Compute zone aware duplicates.
    :param s3_uri: the uri where the crawl data is stored.
    :type s3_uri: str
    :param first_part_id_size: the size of the first partition
    :type first_part_id_size: int
    :param part_id_size: the size of all other partitions
    :type part_id_size: int
    :param tmp_dir: the directory where to save temporary data
    :type tmp_dir: str
    :param force_fetch: if True, the files will be downloaded from s3
    """
    logger.info('Fetching contents stream from S3')
    contents_stream = ContentsStreamDef.load(
        s3_uri,
        tmp_dir=tmp_dir,
        force_fetch=force_fetch
    )
    zone_stream = ZoneStreamDef.load(
        s3_uri,
        tmp_dir=tmp_dir,
        force_fetch=force_fetch
    )
    compliant_urls_stream = CompliantUrlStreamDef.load(
        s3_uri,
        tmp_dir=tmp_dir,
        force_fetch=force_fetch
    )

    generator = get_context_aware_duplicate_metadata(
        contents_stream,
        zone_stream,
        compliant_urls_stream
    )
    generator = itertools.imap(to_string, generator)
    files = ContentsContextAwareDuplicateStreamDef.persist(
        generator,
        s3_uri,
        first_part_size=first_part_id_size,
        part_size=part_id_size
    )
    return files
