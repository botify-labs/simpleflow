from cdf.core.insights import Insight, PositiveTrend
from cdf.query.filter import (
    EqFilter,
    GtFilter,
    AndFilter,
    BetweenFilter)
from cdf.query.sort import DescendingSort
from cdf.query.aggregation import AvgAggregation, SumAggregation
from cdf.metadata.url.url_metadata import FLOAT_TYPE
from cdf.features.ganalytics.streams import _iterate_sources
from cdf.core.metadata.constants import RENDERING


def get_medium_source_insights(medium, source):
    """Return all the Google Analytics insights related to a given
    (medium, source) tuple
    :param medium: the traffic medium to consider
    :type medium: str ("organic" or "social")
    :param source: the traffic source to consider
    :type source : str (from ORGANIC_SOURCES or SOCIAL_SOURCES)
    :returns: list - list of Insight
    """
    result = []
    result.extend(get_ganalytics_main_metric_insights(medium, source))
    result.extend(get_strategic_active_insights(medium, source))
    result.extend(get_strategic_visit_nb_range_insights(medium, source))
    return result


def get_ganalytics_main_metric_insights(medium, source):
    """Return the Google Analytics insights related to the main metric
    for a given (medium, source) tuple
    :param medium: the traffic medium to consider
    :type medium: str ("organic" or "social")
    :param source: the traffic source to consider
    :type source : str (from ORGANIC_SOURCES or SOCIAL_SOURCES)
    :returns: list - list of Insight
    """
    name_prefix = "{}_{}".format(medium, source)
    visit_field = "visits.{}.{}.nb".format(medium, source)
    return [
        Insight(
            "{}_visits_nb_avg".format(name_prefix),
            "Average Visits by Active URL",
            PositiveTrend.UNKNOWN,
            GtFilter(visit_field, 0),
            metric_agg=AvgAggregation(visit_field),
            type=FLOAT_TYPE,
            unit=RENDERING.VISIT
        ),
        Insight(
            "{}_visits_inlinks_avg".format(name_prefix),
            "Average Follow Inlinks by Active URL",
            PositiveTrend.UNKNOWN,
            GtFilter(visit_field, 0),
            metric_agg=AvgAggregation(
                "inlinks_internal.nb.unique"
            ),
            type=FLOAT_TYPE,
            unit=RENDERING.LINK
        ),
        Insight(
            "{}_visits_ko_strategic_1_follow_inlink".format(name_prefix),
            "Strategic Not Active URLs with 1 Follow Inlink",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter(visit_field, 0),
                EqFilter("strategic.is_strategic", True),
                EqFilter("inlinks_internal.nb.follow.unique", 1)
            ])
        ),
        Insight(
            "{}_visits_score".format(name_prefix),
            "Number of Visits",
            PositiveTrend.UNKNOWN,
            GtFilter(visit_field, 0),
            additional_fields=[visit_field],
            metric_agg=SumAggregation(visit_field),
            sort_by=DescendingSort(visit_field),
            unit=RENDERING.VISIT
        ),
        Insight(
            "{}_visits".format(name_prefix),
            "Active URLs",
            PositiveTrend.UNKNOWN,
            GtFilter(visit_field, 0),
            additional_fields=[visit_field],
            sort_by=DescendingSort(visit_field)
        )
    ]


def get_strategic_active_insights(medium, source):
    """For a given (medium, source) tuple, compute the count of URLs
    for all the possible combinations of "is_strategic" and "is_active"
    :param medium: the traffic medium to consider
    :type medium: str ("organic" or "social")
    :param source: the traffic source to consider
    :type source : str (from ORGANIC_SOURCES or SOCIAL_SOURCES)
    :returns: list - list of Insight
    """
    name_prefix = "{}_{}".format(medium, source)
    visit_field = "visits.{}.{}.nb".format(medium, source)
    strategic_field = "strategic.is_strategic"
    return [
        Insight(
            "{}_visits_strategic".format(name_prefix),
            "Strategic Active URLs",
            PositiveTrend.UNKNOWN,
            AndFilter([
                GtFilter(visit_field, 0),
                EqFilter(strategic_field, True)
            ]),
            additional_fields=[visit_field],
            sort_by=DescendingSort(visit_field)
        ),
        Insight(
            "{}_visits_not_strategic".format(name_prefix),
            "Not Strategic Not Active URLs",
            PositiveTrend.UNKNOWN,
            AndFilter([
                GtFilter(visit_field, 0),
                EqFilter(strategic_field, False)
            ]),
            additional_fields=[visit_field],
            sort_by=DescendingSort(visit_field)
        ),
        Insight(
            "{}_visits_ko_strategic".format(name_prefix),
            "Strategic Not Active URLs",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter(visit_field, 0),
                EqFilter(strategic_field, True)
            ])
        ),
        Insight(
            "{}_visits_ko_not_strategic".format(name_prefix),
            "Not Strategic Not Active URLs",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter(visit_field, 0),
                EqFilter(strategic_field, False)
            ])
        )
    ]


def get_strategic_visit_nb_range_insights(medium, source):
    """Return the Google Analytics insights related
    to the active strategic URLs for a given (medium, source) tuple
    :param medium: the traffic medium to consider
    :type medium: str ("organic" or "social")
    :param source: the traffic source to consider
    :type source : str (from ORGANIC_SOURCES or SOCIAL_SOURCES)
    :returns: list - list of Insight
    """
    name_prefix = "{}_{}".format(medium, source)
    visit_field = "visits.{}.{}.nb".format(medium, source)
    strategic_field = "strategic.is_strategic"
    return [
        Insight(
            "{}_visits_strategic_1".format(name_prefix),
            "Strategic Active URLs with 1 Visit",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter(strategic_field, True),
                EqFilter(visit_field, 1)
            ]),
            additional_fields=[visit_field],
            sort_by=DescendingSort(visit_field)
        ),
        Insight(
            "{}_visits_strategic_2_5".format(name_prefix),
            "Strategic Active URLs with 2 to 5 Visits",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter(strategic_field, True),
                BetweenFilter(visit_field, [2, 5])
            ]),
            additional_fields=[visit_field],
            sort_by=DescendingSort(visit_field)
        ),
        Insight(
            "{}_visits_strategic_6_10".format(name_prefix),
            "Strategic Active URLs with 6 to 10 Visits",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter(strategic_field, True),
                BetweenFilter(visit_field, [6, 10])
            ]),
            additional_fields=[visit_field],
            sort_by=DescendingSort(visit_field)
        ),
        Insight(
            "{}_visits_strategic_11_100".format(name_prefix),
            "Strategic Active URLs with 11 to 100 Visits",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter(strategic_field, True),
                BetweenFilter(visit_field, [11, 100])
            ]),
            additional_fields=[visit_field],
            sort_by=DescendingSort(visit_field)
        ),
        Insight(
            "{}_visits_strategic_gt_100".format(name_prefix),
            "Strategic Active URLs with +100 Visits",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter(strategic_field, True),
                GtFilter(visit_field, 100)
            ]),
            additional_fields=[visit_field],
            sort_by=DescendingSort(visit_field)
        )
    ]


def get_ganalytics_insights():
    """Return all the Google Analytics insights
    :returns: list - list of Insight
    """
    result = []
    for medium, source in _iterate_sources():
        result.extend(get_medium_source_insights(medium, source))
    return result


insights = get_ganalytics_insights()
