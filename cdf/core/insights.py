from cdf.query.aggregation import CountAggregation

class Insight(object):
    """A class to represent an insight
    An insight is a number that will be displayed on the report.
    It corresponds to a number of urls.
    Each insight has a corresponding elasticsearch query that is used to
    compute its value
    """
    def __init__(self, identifier, name, input_filter=None, metric_agg=None):
        """Constructor
        :param identifier: the insight identifier (short)
        :type identifier: str
        :param name: the insight name (displayed on the report)
        :param input_filter: the filter to apply for the botify query
        :type input_filter: Filter
        :param metric_agg: the aggregation to compute for the botify query.
                           If None, use a simple count on the urls
        :type metric_agg: MetricAggregation
        """
        self.identifier = identifier
        self.name = name
        self.filter = input_filter
        self.metric_agg = metric_agg or CountAggregation("url")

    @property
    def query(self):
        """Return the query corresponding to the insight"""
        result = {}
        if self.filter is not None:
            result["filters"] = self.filter.to_dict()
        #self.aggs is alway set (see constructor)
        result["aggs"] = [{'metrics': [self.metric_agg.to_dict()]}]
        return result

    def __repr__(self):
        return "{}: {}".format(self.identifier, self.query)


class InsightValue(object):
    """The value of an insight"""
    def __init__(self, insight, feature_name, trend):
        """Constructor
        :param insight: the corresponding insight
        :type insight: Insight
        :param feature_name: the name of the feature associated with the insight
        :type feature_name: str
        :param trend: the actual list of values.
                      Each element is a InsightTrendPoint and correspond to
                      a crawl id.
        """
        self.insight = insight
        self.feature_name = feature_name
        self.trend = trend

    def to_dict(self):
        """Returns a dict representation of the object
        :returns: dict
        """
        return {
            "identifier": self.insight.identifier,
            "name": self.insight.name,
            "feature": self.feature_name,
            "query": self.insight.query,
            "trend": [trend_point.to_dict() for trend_point in self.trend]
        }

    def __repr__(self):
        return repr(self.to_dict())

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()


class InsightTrendPoint(object):
    """The value of an insight for a given crawl id.
    This value defines a point on the trend curve, hence the class name.
    """
    def __init__(self, crawl_id, value):
        """Constructor
        :param crawl_id: the crawl id which was used to compute the value
        :type crawl_id: int
        :param value: the actual insight value
        :type value: float
        """
        self.crawl_id = crawl_id
        self.value = value

    def to_dict(self):
        """Returns a dict representation of the object
        :returns: dict
        """
        return {
            "crawl_id": self.crawl_id,
            "score": self.value
        }

    def __repr__(self):
        return repr(self.to_dict())


