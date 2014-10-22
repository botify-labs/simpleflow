from enum import Enum
import abc
from cdf.metadata.url.url_metadata import INT_TYPE
from cdf.query.aggregation import CountAggregation
from cdf.core.metadata.constants import RENDERING
from cdf.query.filter import (
    AndFilter,
    OrFilter,
    EqFilter,
    NotFilter,
    ExistFilter
)


class PositiveTrend(Enum):
    '''Indicate whether the positive progression of an insight value
    is up, down or neutral.
    For instance the positive trend for the number of visits is up,
    the positive trend for the average load time is down.
    '''
    UP = 'up'
    DOWN = 'down'
    UNKNOWN = 'unknown'


class AbstractInsight(object):
    """Abstract class to represent insights"""
    __metaclass__ = abc.ABCMeta

    @property
    @abc.abstractmethod
    def query(self):
        """Return the query corresponding to the insight
        :returns: dict"""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def query_to_display(self):
        """Return the query to display to the final user.
        It might be slightly different from the query run on Elasticsearch
        :returns: dict"""
        raise NotImplementedError()


class Insight(AbstractInsight):
    """A class to represent an insight
    An insight is a number that will be displayed on the report.
    It corresponds to a number of urls.
    Each insight has a corresponding elasticsearch query that is used to
    compute its value
    """
    def __init__(self,
                 identifier,
                 name,
                 positive_trend,
                 input_filter=None,
                 metric_agg=None,
                 additional_fields=None,
                 additional_filter=None,
                 sort_by=None,
                 data_type=INT_TYPE,
                 field_type=RENDERING.URL):
        """Constructor
        :param identifier: the insight identifier (short)
        :type identifier: str
        :param name: the insight name (displayed on the report)
        :type name: str
        :param positive_trend: the positive trend for this insight
        :type positive_trend: PositiveTrend
        :param input_filter: the filter to apply for the botify query
        :type input_filter: Filter
        :param metric_agg: the aggregation to compute for the botify query.
                           If None, use a simple count on the urls
        :type metric_agg: MetricAggregation
        :param additional_fields: list of strings that represent fields
                                  that are not part of the query
                                  but that will be displayed on a detailed view.
        :type additional_fields: list
        :param additional_filter: a filter that has no impact on the query but
                                  will be used to filter data in a detailed view.
        :type additional_filter: Filter
        :param sort_by: a sort predicate. It is no impact on the query
                        but will be used to sort the detailed view.
        :type sort_by: Sort
        :param data_type: how the value computed by this insights should be displayed:
                          'float', 'integer', etc.
                          This parameter should be an Enum.
                          It is a string since
                          the base datatypes are not grouped in a enum.
        :type data_type: str
        :param field_type: what type of concept the value represents:
                          "url", "time_sec", etc.
        :type field_type: RENDERING
        """
        self.identifier = identifier
        self.name = name
        self.positive_trend = positive_trend
        self.filter = input_filter
        self.metric_agg = metric_agg or CountAggregation("url")
        self.data_type = data_type
        self.field_type = field_type
        self.additional_fields = additional_fields
        self.additional_filter = additional_filter
        self.sort_by = sort_by

    @property
    def query(self):
        """Return the query corresponding to the insight"""
        result = {}
        if self.filter is not None:
            result["filters"] = self.filter.to_dict()
        #self.aggs is alway set (see constructor)
        result["aggs"] = [{'metrics': [self.metric_agg.to_dict()]}]
        return result

    @property
    def query_to_display(self):
        """Return the query to display to the final user.
        It might be slightly different from the query run on Elasticsearch"""
        return self.query

    def __repr__(self):
        return "{}: {}".format(self.identifier, self.query_to_display)


class ComparisonAwareInsight(AbstractInsight):
    """A decorator that modifies the Elasticsearch queries
    to that they are compatible with crawls with comparisons.
    """
    def __init__(self, insight):
        """Constructor
        :param insight: the insight to decorate
        :type insight: Insight
        """
        self.insight = insight

    def __getattr__(self, name):
        #delegate attribute access to self.insight
        #see Python Cookbook 8.15
        return getattr(self.insight, name)

    @property
    def query(self):
        #select only documents from the current crawl
        filters = OrFilter([
            NotFilter(ExistFilter("disappeared")),
            EqFilter("disappeared", False)
        ])
        if self.insight.filter is not None:
            filters = AndFilter([filters, self.insight.filter])

        result = {}
        result["filters"] = filters.to_dict()
        #self.aggs is alway set (see constructor)
        result["aggs"] = [{'metrics': [self.insight.metric_agg.to_dict()]}]
        return result

    @property
    def query_to_display(self):
        #do not modify the insight query to display
        #we do not want the users to be aware of the comparison tricks.
        return self.insight.query_to_display


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
        result = {
            "identifier": self.insight.identifier,
            "name": self.insight.name,
            "positive_trend": self.insight.positive_trend.value,
            "feature": self.feature_name,
            "query": self.insight.query_to_display,
            "data_type": self.insight.data_type,
            "field_type": self.insight.field_type.value,
            "trend": [trend_point.to_dict() for trend_point in self.trend]
        }
        if self.insight.additional_fields is not None:
            result["additional_fields"] = self.insight.additional_fields

        if self.insight.additional_filter is not None:
            result["additional_filter"] = self.insight.additional_filter.to_dict()

        if self.insight.sort_by is not None:
            result["sort_by"] = self.insight.sort_by.to_dict()

        return result

    def __repr__(self):
        return '<InsightValue: ' + repr(self.to_dict()) + '>'

    def __eq__(self, other):
        if not isinstance(other, InsightValue):
            return False
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
        return '<InsightTrendPoint: ' + repr(self.to_dict()) + '>'


