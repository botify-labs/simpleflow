from cdf.core.insights import Insight, PositiveTrend
from cdf.query.filter import (
    EqFilter,
    GtFilter,
    AndFilter,
    BetweenFilter)
from cdf.query.sort import DescendingSort
from cdf.query.aggregation import AvgAggregation, SumAggregation
from cdf.core.metadata.constants import RENDERING
from cdf.features.ganalytics.streams import _iterate_sources


def get_medium_source_insights(medium, source):
    result = []
    result.extend(get_ganalytics_main_metric_insights(medium, source))
    result.extend(get_strategic_active_insights(medium, source))
    result.extend(get_strategic_visit_nb_range_insight(medium, source))
    return result


def get_ganalytics_main_metric_insights(medium, source):
    name_prefix = "{}_{}".format(medium, source)
    visit_field = "visits.{}.{}.nb".format(medium, source)
    return [
        Insight(
            "{}_visits_nb_avg".format(name_prefix),
            "Average Visits by Active Page",
            PositiveTrend.UNKNOWN,
            GtFilter(visit_field, 0),
            metric_agg=AvgAggregation(visit_field)
        ),
        Insight(
            "{}_visits_inlinks_avg".format(name_prefix),
            "Average Follow Inlinks by Active URL",
            PositiveTrend.UNKNOWN,
            GtFilter(visit_field, 0),
            metric_agg=AvgAggregation(
                "inlinks_internal.nb.unique"
            )
        ),
        Insight(
            "{}_visits_ko_strategic_1_follow_inlink".format(name_prefix),
            "Strategic Not Active URLs with 1 follow link",
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
            sort_by=DescendingSort(visit_field)
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


def get_strategic_visit_nb_range_insight(medium, source):
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
            "{}_visits_strategic_2_10".format(name_prefix),
            "Strategic Active URLs with 2 to 10 Visits",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter(strategic_field, True),
                BetweenFilter(visit_field, [2, 10])
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
    result = []
    for medium, source in _iterate_sources():
        result.extend(get_medium_source_insights(medium, source))
    return result


insights = get_ganalytics_insights()
