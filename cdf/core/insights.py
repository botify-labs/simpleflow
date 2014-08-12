from cdf.query.aggregation import CountAggregation

class Insight(object):
    """A class to represent an insight
    An insight is a number that will be displayed on the report.
    It corresponds to a number of urls.
    Each insight has a corresponding elasticsearch query that is used to
    compute its value
    """
    def __init__(self, identifier, title, input_filter=None, metric_agg=None):
        """Constructor
        :param identifier: the insight identifier (short)
        :type identifier: str
        :param title: the insight title (displayed on the report)
        :param input_filter: the filter to apply for the botify query
        :type input_filter: Filter
        :param metric_agg: the aggregation to compute for the botify query.
                           If None, use a simple count on the urls
        :type metric_agg: MetricAggregation
        """
        self.identifier = identifier
        self.title = title
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
