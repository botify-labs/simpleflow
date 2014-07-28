import os
import itertools
import gzip
import datetime
import json

from cdf.utils.dict import deep_dict
from cdf.features.main.streams import IdStreamDef, InfosStreamDef
from cdf.features.main.utils import get_url_to_id_dict_from_stream
from cdf.features.ganalytics.streams import (RawVisitsStreamDef,
                                             VisitsStreamDef)
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir
from cdf.utils import s3
from cdf.core.constants import FIRST_PART_ID_SIZE, PART_ID_SIZE

from analytics.import_analytics import import_data

from cdf.utils.auth import get_credentials
from cdf.features.ganalytics.matching import match_analytics_to_crawl_urls_stream
from cdf.features.ganalytics.ghost import (build_ghost_counts_dict,
                                           save_ghost_pages,
                                           save_ghost_pages_count,
                                           GoogleAnalyticsAggregator)

@with_temporary_dir
def import_data_from_ganalytics(access_token,
                                refresh_token,
                                ganalytics_site_id,
                                s3_uri,
                                date_start=None,
                                date_end=None,
                                tmp_dir=None,
                                force_fetch=False):
    """
    Request data from google analytics
    TODO (maybe) : take a `refresh_token` instead of an `access_token`
    :param access_token: the access token to retrieve the data from
                         Google Analytics Core Reporting API
    :type access_token: str
    :param refresh_token: the refresh token.
                          The refresh token is used to regenerate an access
                          token when the current one has expired.
    :type refresh_token: str
    :param ganalytics_site_id: the id of the Google Analytics view to retrieve
                               data from.
                               It is an integer with 8 digits.
                               Caution: this is NOT the property id.
                               There may be multiple views for a given property
                               id. (for instance one unfiltered view and one
                               where the traffic from inside the company is
                               filtered).
    :type ganalytics_size_id: int
    :param date_start: Beginning date to retrieve data.
                       If None, the task uses the date from 31 days ago.
                       (so that if both date_start and date_end are None,
                       the import period is the last 30 days)
    :param date_start: date
    :param date_end: Final date to retrieve data.
                     If none, the task uses the date from yesterday.
    :param date_end: date
    :param s3_uri: the uri where to store the data
    :type s3_uri: str
    :param tmp_dir: the path to the tmp directory to use.
                    If None, a new tmp directory will be created.
    :param tmp_dir: str
    :param force_fetch: if True, the files will be downloaded from s3
                        even if they are in the tmp directory.
                        if False, files that are present in the tmp_directory
                        will not be downloaded from s3.
    """

    #set date_start and date_end default values if necessary
    if date_start is None:
        date_start = datetime.date.today() - datetime.timedelta(31)
    if date_end is None:
        date_end = datetime.date.today() - datetime.timedelta(1)
    credentials = get_credentials(access_token, refresh_token)
    import_data(
        "ga:{}".format(ganalytics_site_id),
        credentials,
        date_start,
        date_end,
        tmp_dir
    )
    for f in ['analytics.data.gz', 'analytics.meta.json']:
        s3.push_file(
            os.path.join(s3_uri, f),
            os.path.join(tmp_dir, f)
        )


def get_api_requests(analytics_metadata, ghost_pages_session_count):
    """Build the dict to use to update the API values.
    :param analytics_metadata: a dict that contains metadata about what has be
                               retrieved from Google Analytics Core Reporting API.
    :type analytics_metadata: dict
    :param ghost_pages_session_count: a dict that contains information about the
                                      number of ghost pages per traffic source.
                                      The dict keys have the form :
                                          - "organic.all"
                                          - "social.facebook"
    :type ghost_pages_session_count: dict
    :returns: dict
    """
    return {
        "api_requests": [
            {
                "method": "patch",
                "endpoint_url": "revision",
                "endpoint_suffix": "ganalytics/",
                "data": {
                    "sample_rate": analytics_metadata["sample_rate"],
                    "sample_size": analytics_metadata["sample_size"],
                    "sampled": analytics_metadata["sampled"],
                    "queries_count": analytics_metadata["queries_count"],
                    "ghost": deep_dict(ghost_pages_session_count)
                }
            }
        ]
    }


def load_analytics_metadata(tmp_dir):
    """Load the analytics metadata and returns it as a dict.
    This function was introduced to make test writing easier
    :param tmp_dir: the tmp directory used by the task
    :type tmp_dir: str
    """
    return json.loads(open(os.path.join(tmp_dir, 'analytics.meta.json')).read())


@with_temporary_dir
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

    #create a gzip file to store ambiguous visits
    #we cannot use stream defs as the entries do not have any urlids
    #(by definition)
    #thus they cannot be split.
    ambiguous_urls_filename = 'ambiguous_urls_dataset.gz'
    ambiguous_urls_filepath = os.path.join(tmp_dir,
                                           ambiguous_urls_filename)

    with gzip.open(ambiguous_urls_filepath, 'wb') as ambiguous_urls_file:

        stream = RawVisitsStreamDef.get_stream_from_s3_path(
            os.path.join(s3_uri, 'analytics.data.gz'),
            tmp_dir=tmp_dir,
            force_fetch=force_fetch
        )

        ghost_pages_aggregator = match_analytics_to_crawl_urls_stream(stream,
                                                                      url_to_id,
                                                                      urlid_to_http_code,
                                                                      dataset,
                                                                      ambiguous_urls_file)
        top_ghost_pages = ghost_pages_aggregator.top_pages
        ghost_pages_session_count = ghost_pages_aggregator.session_count
        ghost_pages_url_count = ghost_pages_aggregator.url_count

    #save top ghost pages in dedicated files
    save_top_pages(top_ghost_pages, s3_uri, "top_ghost_pages", tmp_dir)

    #mix url counts and session counts dictionaries
    ghost_pages_count = build_ghost_counts_dict(
        ghost_pages_session_count,
        ghost_pages_url_count
    )
    #save session & url counts for ghost pages
    session_count_path = save_ghost_pages_count(
        ghost_pages_count,
        tmp_dir)
    s3.push_file(
        os.path.join(s3_uri, os.path.basename(session_count_path)),
        session_count_path
    )

    s3.push_file(
        os.path.join(s3_uri, ambiguous_urls_filename),
        ambiguous_urls_filepath
    )
    dataset.persist_to_s3(s3_uri,
                          first_part_id_size=first_part_id_size,
                          part_id_size=part_id_size)

    #download analytics.meta.json (generated by import_data_from_ganalytics())
    s3.fetch_file(
        os.path.join(s3_uri, 'analytics.meta.json'),
        os.path.join(tmp_dir, 'analytics.meta.json'),
        force_fetch)

    analytics_metadata = load_analytics_metadata(tmp_dir)
    api_requests = get_api_requests(analytics_metadata,
                                    ghost_pages_count)
    # Advise the workflow that we need to send data to the remote db
    # through the api by calling a feature endpoint (prefixed by its revision)
    return api_requests


def save_top_pages(top_pages, s3_uri, prefix, tmp_dir):
    """Save top pages on s3
    :param top_pages: a dict source/medium -> top_pages with top_pages
                      a list of tuples (count, url)
    :type top_pages: dict
    :param s3_uri: the uri where to save the data
    :type s3_uri: str
    :param tmp_dir: a tmp dir for storing files
    :type tmp_dir: str
    """
    #save top ghost pages in dedicated files
    file_paths = []
    for key, values in top_pages.iteritems():
        #convert the heap into a sorted list
        values = sorted(values, reverse=True)
        #protocol is missing, we arbitrarly prefix all the urls with http
        values = [(count, "http://{}".format(url)) for count, url in values]
        #create a dedicated file
        crt_ghost_file_path = save_ghost_pages(key, values, prefix, tmp_dir)
        file_paths.append(crt_ghost_file_path)

    #push top ghost files to s3
    for ghost_file_path in file_paths:
        s3.push_file(
            os.path.join(s3_uri, os.path.basename(ghost_file_path)),
            ghost_file_path
        )
