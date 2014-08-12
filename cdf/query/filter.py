from abc import ABCMeta, abstractmethod


class Filter(object):
    """An abstract class to represent a filter in a query.
    The Filter and its derivates defines a Composite pattern.
    Concrete filters defines actual filter: "eq", "gte", etc
    Filters can be combined with operators: "and", "or", etc
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def to_dict(self):
        """A method to get a dict that represent the filter.
        The dict can then be used as the "filters" part of an elasticsearch
        query
        :returns: dict
        """
        raise NotImplementedError()


class FilterCombination(Filter):
    """An class to represent the combination of multiple filters
    """
    def __init__(self, predicate, filters):
        """Constructor
        :param predicate: the predicate to use to combine the filters
        :type predicate: str
        :param filters: the filters to combine as a list of Filter
        :type filters: list
        """
        self.predicate = predicate
        self.filters = filters

    def to_dict(self):
        return {
            self.predicate: [f.to_dict() for f in self.filters]
        }


class AndFilter(FilterCombination):
    """A class to combine filters with a "and" """
    def __init__(self, filters):
        super(self.__class__, self).__init__("and", filters)


class OrFilter(FilterCombination):
    """A class to combine filters with a "or" """
    def __init__(self, filters):
        super(self.__class__, self).__init__("or", filters)


class NotFilter(Filter):
    """A negation filter"""
    def __init__(self, filter):
        """Constructor
        :param filter: the input filter
        :type filter: Filter
        """
        self.filter = filter

    def to_dict(self):
        return {"not": self.filter.to_dict()}


class ConcreteFilter(Filter):
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


class EqFilter(ConcreteFilter):
    """An filter for "eq" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "eq", value)


class LtFilter(ConcreteFilter):
    """A filter for "less than" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "lt", value)


class LteFilter(ConcreteFilter):
    """A filter for "less than or equal" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "lte", value)


class GtFilter(ConcreteFilter):
    """A filter for "greater than" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "gt", value)


class GteFilter(ConcreteFilter):
    """A filter for "greater than or equal" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "gte", value)


class BetweenFilter(ConcreteFilter):
    """An filter for "between" predicate"""
    def __init__(self, field, value):
        super(self.__class__, self).__init__(field, "between", value)

