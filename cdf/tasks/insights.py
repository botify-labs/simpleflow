import json

from cdf.utils.s3 import push_content

from cdf.query.query import Query
from cdf.core.features import Feature
from cdf.core.insights import InsightValue, InsightTrendPoint
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir


def get_query_agg_result(query):
    """Return the aggregation part of of a query result
    :param query: the input query
    :type query: Query
    :returns: float
    """
    #if the result is empty query.aggs equals {}
    #in this case we return 0
    #cf https://github.com/sem-io/botify-cdf/issues/521
    if isinstance(query.aggs, dict):
        return 0
    else:
        return query.aggs[0]["metrics"][0]


def compute_insight_value(insight,
                          feature_name,
                          crawl_ids,
                          es_location,
                          es_index):
    """Compute the value of an insight
    :param insight: the insight to compute
    :type insight: Insight
    :param feature_name: the name of the feature associated with the insight
    :type feature_name: str
    :param crawl_ids: the list of crawl_ids for which the insight value
                      is to be computed. Each element is an integer.
    :type crawl_ids: list
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
    for crawl_id in crawl_ids:
        query = Query(es_location,
                      es_index,
                      es_doc_type,
                      crawl_id,
                      revision_number,
                      insight.query)
        trend_point = InsightTrendPoint(crawl_id, get_query_agg_result(query))
        trend.append(trend_point)
    return InsightValue(insight, feature_name, trend)


def get_features(feature_names):
    """Return the list of features given their names
    :param feature_names: the feature names as a list of strings
    :type feature_names: list
    :returns: list - a list of Feature objects
    :raises: ValueError - if a feature is missing
    """
    result = [feature for feature in Feature.get_features() if
              feature.identifier in feature_names]
    if len(result) != len(set(feature_names)):
        missing_feature_names = set.difference(
            set(feature_names),
            [f.identifier for f in Feature.get_features()]
        )
        raise ValueError(
            "Features: {} were not found".format(missing_feature_names)
        )
    return result


def compute_insight_values(crawl_ids, features, es_location, es_index):
    """Compute the insight values for a set of crawl ids and a set of features.
    :param crawl_ids: the list of crawl ids to use to compute the insights.
    :type crawl_ids: list
    :param features: the list of Feature objects
                     for which to compute the insights.
    :type feature: list
    :param es_location: the location of the elasticsearch server.
                        For instance "http://elasticsearch1.staging.saas.botify.com:9200"
    :type es_location: str
    :param es_index: the name of the elasticsearch index to use.
                     Usually "botify".
    :type es_index: str
    :returns: list - a list of InsightValue
    """
    result = []
    for feature in features:
        for insight in feature.get_insights():
            insight_value = compute_insight_value(insight,
                                                  feature.name,
                                                  crawl_ids,
                                                  es_location,
                                                  es_index)
            result.append(insight_value)
    return result


@with_temporary_dir
def compute_insights(crawl_ids,
                     feature_names,
                     es_location,
                     es_index,
                     s3_uri,
                     tmp_dir=None,
                     force_fetch=False):
    """A task to compute the insights and push their values to s3
    as a json file.
    :param crawl_ids: the list of crawl ids to use to compute the insights.
    :type crawl_ids: list
    :param feature_names: the list of feature names
                          for which to compute the insights.
                          For instance : ["main", "semantic_metadata"]
    :type feature_names: list
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
    """
    features = get_features(feature_names)
    result = compute_insight_values(crawl_ids, features, es_location, es_index)

    push_content(
        "{}/insights.json".format(s3_uri),
        json.dumps([insight.to_dict() for insight in result])
    )
