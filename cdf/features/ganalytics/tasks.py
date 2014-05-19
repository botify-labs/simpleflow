import os

from cdf.features.main.streams import IdStreamDef, InfosStreamDef
from cdf.features.main.utils import get_url_to_id_dict_from_stream
from cdf.features.ganalytics.streams import (RawVisitsStreamDef,
                                             VisitsStreamDef,
                                             AmbiguousVisitsStreamDef)
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir
from cdf.utils import s3
from cdf.core.constants import FIRST_PART_ID_SIZE, PART_ID_SIZE
from cdf.core.decorators import feature_enabled

from analytics.import_analytics import import_data

from cdf.utils.auth import get_credentials
from cdf.features.ganalytics.matching import MATCHING_STATUS, get_urlid

@with_temporary_dir
@feature_enabled('ganalytics')
def import_data_from_ganalytics(access_token, refresh_token, ganalytics_site_id, s3_uri,
                                tmp_dir=None, force_fetch=False):
    """
    Request data from google analytics
    TODO (maybe) : take a `refresh_token` instead of an `access_token`
    """
    credentials = get_credentials(access_token, refresh_token)
    import_data(
        "ga:{}".format(ganalytics_site_id),
        credentials,
        tmp_dir
    )
    for f in ['analytics.data.gz', 'analytics.meta.json']:
        s3.push_file(
            os.path.join(s3_uri, f),
            os.path.join(tmp_dir, f)
        )


@with_temporary_dir
@feature_enabled('ganalytics')
def match_analytics_to_crawl_urls(s3_uri, first_part_id_size=FIRST_PART_ID_SIZE, part_id_size=PART_ID_SIZE,
                                  protocol='http', tmp_dir=None, force_fetch=False):
    """
    :param raw_data_file : the google analytics raw data file
    :pram s3_uri : the root storage uri where to find the given crawl dataset

    Transform a row file like :
    www.site.com/my_url organic google 12
    www.site.com/my_another_url organic google 50

    To :
    576 organic google 12
    165 organic google 50
    """
    id_stream = IdStreamDef.get_stream_from_s3(s3_uri, tmp_dir=tmp_dir)
    info_stream = InfosStreamDef.get_stream_from_s3(s3_uri, tmp_dir=tmp_dir)
    id_idx = InfosStreamDef.field_idx("id")
    http_code_idx = InfosStreamDef.field_idx("http_code")
    urlid_to_http_code = {s[id_idx]: s[http_code_idx] for s in info_stream}

    url_to_id = get_url_to_id_dict_from_stream(id_stream)
    dataset = VisitsStreamDef.create_temporary_dataset()
    ambiguous_urls_dataset = AmbiguousVisitsStreamDef.create_temporary_dataset()

    stream = RawVisitsStreamDef.get_stream_from_s3_path(os.path.join(s3_uri, 'analytics.data.gz'), tmp_dir=tmp_dir, force_fetch=force_fetch)
    for entry in stream:
        url_id, matching_status = get_urlid(entry, url_to_id, urlid_to_http_code)
        if url_id:
            dataset_entry = list(entry)
            dataset_entry[0] = url_id
            dataset.append(*dataset_entry)
            if matching_status == MATCHING_STATUS.AMBIGUOUS:
                ambiguous_urls_dataset.append(*dataset_entry)

    ambiguous_urls_dataset.persist_to_s3(s3_uri,
                                         first_part_id_size=first_part_id_size,
                                         part_id_size=part_id_size)
    dataset.persist_to_s3(s3_uri,
                          first_part_id_size=first_part_id_size,
                          part_id_size=part_id_size)
