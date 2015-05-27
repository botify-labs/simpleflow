from cdf.core.constants import FIRST_PART_ID_SIZE, PART_ID_SIZE
from cdf.features.main.streams import IdStreamDef
from cdf.tasks.constants import DEFAULT_FORCE_FETCH
from cdf.tasks.decorators import with_temporary_dir
from cdf.utils import s3
from cdf.utils.remote_files import (
    get_crawl_info, load_crawler_metakeys, get_max_crawled_urlid
)
from .get_urls import get_urls_with_same_kv
from .streams import DuplicateQueryKVsStreamDef


@with_temporary_dir
def task_get_urls_with_same_kv(s3_uri,
                               first_part_id_size=FIRST_PART_ID_SIZE,
                               part_id_size=PART_ID_SIZE,
                               tmp_dir=None,
                               force_fetch=DEFAULT_FORCE_FETCH):
    # HACK
    if s3.is_s3_uri(s3_uri):
        crawler_metakeys = get_crawl_info(s3_uri, tmp_dir=tmp_dir)
    else:
        crawler_metakeys = load_crawler_metakeys(s3_uri)
    max_crawled_urlid = get_max_crawled_urlid(crawler_metakeys)

    urlids = IdStreamDef.load(s3_uri, tmp_dir=tmp_dir, force_fetch=force_fetch)
    prob_uids = get_urls_with_same_kv(urlids, max_crawled_urlid)

    DuplicateQueryKVsStreamDef.persist(
        prob_uids,
        s3_uri,
        first_part_size=first_part_id_size,
        part_size=part_id_size
    )
