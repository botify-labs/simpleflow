class Insight(object):
    """A class to represent an insight
    An insight is a number that will be displayed on the report.
    It corresponds to a number of urls.
    Each insight has a corresponding elasticsearch query that is used to
    compute its value
    """
    def __init__(self, identifier, title, es_query):
        """Constructor
        :param identifier: the insight identifier (short)
        :type identifier: str
        :param title: the insight title (displayed on the report)
        :param es_query: the elasticsearch query
        :type es_query: dict
        """
        self.identifier = identifier
        self.title = title
        self.es_query = es_query
