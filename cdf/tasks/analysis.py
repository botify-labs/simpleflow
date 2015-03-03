import json
import time
import requests
import logging
from elasticsearch import Elasticsearch

from cdf.core.metadata.dataformat import generate_data_format
from cdf.exceptions import ApiError, ApiFormatError, BotifyQueryException
from cdf.metadata.url.es_backend_utils import ElasticSearchBackend
from cdf.utils.s3 import push_content
from cdf.utils.auth import get_botify_api_token
from cdf.query.query import Query
from cdf.core.features import Feature
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir


logger = logging.getLogger(__name__)


# TODO maybe put it in a util module
def refresh_index(es_location, es_index):
    """Issues a `refresh` request to ElasticSearch cluster

    :param es_location: ElasticSearch cluster location
    :type es_location: str
    :param es_index: name of the index to refresh
    :type es_index: str
    :return: refresh result
    :rtype: dict
    """
    #here we set a timeout of 300 instead of the default 10, because it often
    #happens that the cluster won't synchronize all data under load or while
    #recovering/relocating shards.. common observed values are 30s as of this
    #writing so we should be safe with 300, and we really don't care if this
    #task takes a few minutes to complete
    #TODO: instrument it so we can graph response times
    es = Elasticsearch(es_location, timeout=300)
    return es.indices.refresh(index=es_index)


def get_feature_options(crawl_configurations):
    """Return the feature options corresponding to a list of crawl ids.
    Feature options are retrieved through the API.
    :param crawl_configurations: the list of crawl configurations to consider
                                 as a list of tuples (crawl_id, config_endpoint, s3_uri)
    :type crawl_configurations: list
    :returns: dict - a dict crawl_id -> feature_options
    :raises: ApiError - if one API call fails
    :raises: ApiFormatError - if one API answer does not have the expected format.
    :raises: ConfigurationError - if there was an error when getting authentication token.
    """
    result = {}
    headers = {
        "Authorization": "Token {}".format(get_botify_api_token())
    }
    for crawl_id, config_endpoint, s3_uri in crawl_configurations:
        r = requests.get(config_endpoint, headers=headers)
        if not r.ok:
            raise ApiError("{}: {}".format(r.status_code, r.reason))
        if "features" not in r.json():
            raise ApiFormatError(
                "'features' entry is missing in '{}'".format(r.json())
            )
        #NB this is not exactly the feature options
        #as we get them in the context.
        #but it should be ok.
        feature_options = r.json()["features"]
        result[crawl_id] = feature_options
    return result
