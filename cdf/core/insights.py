class Insight(object):
    """A class to represent an insight
    An insight is a number that will be displayed on the report.
    It corresponds to a number of urls.
    Each insight has a corresponding elasticsearch query that is used to
    compute its value
    """
    def __init__(self, identifier, title, input_filter=None, aggs=None):
        """Constructor
        :param identifier: the insight identifier (short)
        :type identifier: str
        :param title: the insight title (displayed on the report)
        :param input_filter: the filter to apply for the elastic search query
        :type input_filter: Filter
        :param aggs: the aggregations to compute for the elastic search query.
                     If None, use a simple count
        :type aggs: dict
        """
        self.identifier = identifier
        self.title = title
        self.filter = input_filter
        self.aggs = aggs or [{'metrics': ["count"]}]

    @property
    def es_query(self):
        """Return the elasticsearch query corresponding to the insight"""
        result = {}
        if self.filter is not None:
            result["filters"] = self.filter.to_dict()
        if self.aggs is not None:
            result["aggs"] = self.aggs
        return result

    def __repr__(self):
        return "{}: {}".format(self.identifier, self.es_query)
