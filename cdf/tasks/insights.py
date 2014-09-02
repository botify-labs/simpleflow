import json

from urlparse import urlparse, urljoin
import requests
from elasticsearch import Elasticsearch

from cdf.exceptions import ApiError, ApiFormatError
from cdf.utils.s3 import push_content
from cdf.query.query import Query
from cdf.core.features import Feature
from cdf.core.insights import InsightValue, InsightTrendPoint
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir


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
    es = Elasticsearch(es_location)
    return es.indices.refresh(index=es_index)


def get_query_agg_result(query):
    """Return the aggregation part of of a query result
    :param query: the input query
    :type query: Query
    :returns: float
    """
    #if the result is empty query.aggs equals []
    #in this case we return 0
    if len(query.aggs) == 0:
        return 0
    else:
        return query.aggs[0]["metrics"][0]


def compute_insight_value(insight,
                          feature_name,
                          crawls,
                          es_location,
                          es_index):
    """Compute the value of an insight
    :param insight: the insight to compute
    :type insight: Insight
    :param feature_name: the name of the feature associated with the insight
    :type feature_name: str
    :param crawls: the list of crawl ids to use to compute the insights.
    :type crawls: list
    :param es_location: the location of the elasticsearch server.
                        For instance "http://elasticsearch1.staging.saas.botify.com:9200"
    :type es_location: str
    :param es_index: the name of the elasticsearch index to use.
                     Usually "botify".
    :type es_index: str
    :returns: InsightValue
    """
    es_doc_type = "urls"
    #TODO check if using 0 is ok.
    revision_number = 0
    trend = []
    for crawl_id, feature_options in crawls:
        #TODO use feature_options in Query constructor
        query = Query(es_location,
                      es_index,
                      es_doc_type,
                      crawl_id,
                      revision_number,
                      insight.query)
        trend_point = InsightTrendPoint(crawl_id,
                                        get_query_agg_result(query))
        trend.append(trend_point)
    return InsightValue(insight, feature_name, trend)


def compute_insight_values(crawls, es_location, es_index):
    """Compute the insight values for a set of crawl ids and a set of features.
    :param crawls: the list of crawls to use to compute the insights.
                   Each crawl is a tuple (crawl_id, feature_options)
                   with
                   - crawl_id: an integer
                   - feature_options: a dict containing the feature options.
    :type crawls: list
    :param es_location: the location of the elasticsearch server.
                        For instance "http://elasticsearch1.staging.saas.botify.com:9200"
    :type es_location: str
    :param es_index: the name of the elasticsearch index to use.
                     Usually "botify".
    :type es_index: str
    :returns: list - a list of InsightValue
    """
    result = []
    for feature in Feature.get_features():
        for insight in feature.get_insights():
            insight_value = compute_insight_value(insight,
                                                  feature.name,
                                                  crawls,
                                                  es_location,
                                                  es_index)
            result.append(insight_value)
    return result


@with_temporary_dir
def compute_insights(crawls,
                     es_location,
                     es_index,
                     s3_uri,
                     tmp_dir=None,
                     force_fetch=False):
    """A task to compute the insights and push their values to s3
    as a json file.
    :param crawls: the list of crawls to use to compute the insights.
                   Each crawl is a tuple (crawl_id, feature_options)
                   with
                   - crawl_id: an integer
                   - feature_options: a dict containing the feature options.
    :type crawls: list
    :param es_location: the location of the elasticsearch server.
                        For instance "http://elasticsearch1.staging.saas.botify.com:9200"
    :type es_location: str
    :param es_index: the name of the elasticsearch index to use.
                     Usually "botify".
    :param s3_uri: the s3 uri where the crawl data is stored.
    :type s3_uri: str
    :param user_agent: the user agent to use for the query.
    :type user_agent: str
    :param tmp_dir: the path to the directory where to save the files
    :type tmp_dir: str
    :param force_fetch: if True, the files will be downloaded from s3
    :type force_fetch: bool
    :returns: str - the uri of the generated json document
    """
    result = compute_insight_values(crawls, es_location, es_index)

    destination_uri = "{}/precomputation/insights.json".format(s3_uri)
    push_content(
        destination_uri,
        json.dumps([insight_value.to_dict() for insight_value in result])
    )
    return destination_uri


def get_api_address(crawl_endpoint):
    """Return the API address given the API crawl endpoit.
    This function is somehow a hack made necessary
    by the fact that the analysis context does not contain the API address
    :param crawl_endpoint: the crawl endpoint (ex: http://api.staging.botify.com/crawls/1540/revisions/1568/)
    :type crawl_endpoint: str
    :returns: str
    """
    parsed_url = urlparse(crawl_endpoint)
    return "{}://{}".format(parsed_url.scheme, parsed_url.netloc)


def get_feature_options(api_address, crawl_ids):
    """Return the feature options corresponding to a list of crawl ids.
    Feature options are retrieved through the API.
    :param api_address: the API address (ex: http://api.botify.com"
    :type api_address: str
    :param crawl_ids: the list of crawl ids to consider as a list of ints.
    :type crawl_ids: list
    :returns: list - a list of dict, each dict corresponding to a set of
                     feature_options
    :raises: ApiError - if one API call fails
    :raises: ApiFormatError - if one API answer does not have the expected format.
    """
    result = []
    for crawl_id in crawl_ids:
        endpoint = urljoin(api_address, "crawls/{}/".format(crawl_id))
        r = requests.get(endpoint)
        if not r.ok:
            raise ApiError("{}: {}".format(r.status_code, r.reason))
        if "features" not in r.json():
            raise ApiFormatError(
                "'features' entry is missing in '{}'".format(r.json())
            )
        feature_options = r.json()["features"]
        result.append(feature_options)
    return result
