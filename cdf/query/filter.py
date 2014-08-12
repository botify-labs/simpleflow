class Filter(object):
    """A class to represent a filter in a query.
    The use of a dedicated class
    - saves code
    - document what are the expected fields
    - document the query language
    For instance instead of writing
    {
        "field": "http_code",
        "predicate": "eq",
        "value": 200
    }
    to build a query
    you can now write
      Filter("http_code", "eq", 200).to_dict()
    Thanks to the class you know that a field, an operator and a value are
    expected.
    And you don't have to remember that the expected key for the field is "field".
    """

    def __init__(self, field, predicate, value):
        """Constructor
        :param field: the field concerned by the filter
        :type field: str
        :param predicate: the filter predicate (for instance: "eq", "lt", "between")
        :type operator: str
        :param value: the value associated with the operator
                      for instance 300 if predicate is "eq"
                      or [300, 399] if predicate is "between"
        :type value: object
        """
        self.field = field
        self.predicate = predicate
        self.value = value

    def to_dict(self):
        """Return a dict version of the predicate.
        Usually, this representation is used to build filters for queries.
        :returns: dict
        """
        return {
            "field": self.field,
            "predicate": self.predicate,
            "value": self.value
        }

    def __repr__(self):
        return repr(self.to_dict())


class EqFilter(Filter):
    """An filter for "eq" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "eq", value)


class LtFilter(Filter):
    """A filter for "less than" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "lt", value)


class LteFilter(Filter):
    """A filter for "less than or equal" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "lte", value)


class GtFilter(Filter):
    """A filter for "greater than" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "gt", value)


class GteFilter(Filter):
    """A filter for "greater than or equal" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "gte", value)


class BetweenFilter(Filter):
    """An filter for "between" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "between", value)

