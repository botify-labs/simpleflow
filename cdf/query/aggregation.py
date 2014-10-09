class MetricAggregation(object):
    """A class to represent the aggregation on one metric"""
    def __init__(self, predicate, field):
        """Constructor
        :param predicate: the aggregation operation.
                          For instance: "min", "max", etc.
        :type predicate: str
        :param field: the metric to use for the aggregation
        :type field: str
        """
        self.predicate = predicate
        self.field = field

    def to_dict(self):
        """Returns a dict representation of the object.
        The dict is typically used to build a botify query
        :returns: dict"""
        return {self.predicate: self.field}


class AvgAggregation(MetricAggregation):
    """A specialization of MetricAggregation for the "avg" operator"""
    def __init__(self, field):
        super(self.__class__, self).__init__("avg", field)


class MinAggregation(MetricAggregation):
    """A specialization of MetricAggregation for the "min" operator"""
    def __init__(self, field):
        super(self.__class__, self).__init__("min", field)


class MaxAggregation(MetricAggregation):
    """A specialization of MetricAggregation for the "max" operator"""
    def __init__(self, field):
        super(self.__class__, self).__init__("max", field)


class CountAggregation(MetricAggregation):
    """A specialization of MetricAggregation for the "count" operator"""
    def __init__(self, field):
        super(self.__class__, self).__init__("count", field)


class SumAggregation(MetricAggregation):
    """A specialization of MetricAggregation for the "sum" operator"""
    def __init__(self, field):
        super(self.__class__, self).__init__("sum", field)
