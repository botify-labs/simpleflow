from cdf.core.insights import Insight, PositiveTrend
from cdf.query.filter import (
    EqFilter,
    GtFilter,
    AndFilter,
    OrFilter,
    BetweenFilter,
    NotFilter,
    ExistFilter)
from cdf.metadata.url.url_metadata import FLOAT_TYPE
from cdf.query.sort import DescendingSort
from cdf.query.aggregation import AvgAggregation, SumAggregation


def get_average_inlinks_insights():
    """Return insights related to inlinks averages.
    :returns: list - list of Insight
    """
    return [
        Insight(
            "inlinks_avg",
            "Average Inlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=AvgAggregation("inlinks_internal.nb.unique"),
            data_type=FLOAT_TYPE
        ),
        Insight(
            "inlinks_avg_follow",
            "Average Follow Inlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=AvgAggregation("inlinks_internal.nb.follow.unique"),
            data_type=FLOAT_TYPE
        ),
    ]


def get_inlinks_sum_insights():
    """Return insights related to inlinks sums.
    :returns: list - list of Insight
    """
    return [
        Insight(
            "inlinks_sum",
            "Total Number of Inlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=SumAggregation("inlinks_internal.nb.unique")
        ),
        Insight(
            "inlinks_sum_follow",
            "Total Number of Follow Inlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=SumAggregation("inlinks_internal.nb.follow.unique")
        ),
        Insight(
            "inlinks_sum_nofollow",
            "Total Number of Nofollow Inlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=SumAggregation("inlinks_internal.nb.nofollow.unique")
        )
    ]


def get_outlinks_sum_insights():
    """Return insights related to outlinks sums.
    :returns: list - list of Insight
    """
    return [
        Insight(
            "outlinks_internal_sum_follow",
            "Total Number of Internal Follow Outlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=SumAggregation("inlinks_internal.nb.follow.unique")
        ),
        Insight(
            "outlinks_external_sum_follow",
            "Total Number of External Follow Outlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=SumAggregation("outlinks_external.nb.follow.unique")
        ),
        Insight(
            "outlinks_internal_sum_nofollow",
            "Total Number of Internal Nofollow Outlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=SumAggregation("inlinks_internal.nb.nofollow.unique")
        ),
        Insight(
            "outlinks_external_sum_nofollow",
            "Total Number of External Nofollow Outlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=SumAggregation("outlinks_external.nb.nofollow.unique")
        ),
        Insight(
            "outlinks_errors_sum",
            "Total Number of Broken Follow Outlinks",
            PositiveTrend.DOWN,
            metric_agg=SumAggregation("outlinks_errors.non_strategic.nb")
        )
    ]


def get_inlinks_range_insights():
    """Return insights related to the URLs that receive
    a given number of inlinks.
    :returns: list - list of Insight
    """
    field = "inlinks_internal.nb.follow.unique"
    return [
        Insight(
            "inlinks_follow_1",
            "URLs 1 Follow Inlink",
            PositiveTrend.DOWN,
            EqFilter(field, 1),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        ),
        Insight(
            "inlinks_follow_2_5",
            "URLs Between 2 and 5 Follow Inlinks",
            PositiveTrend.UNKNOWN,
            BetweenFilter(field, [2, 5]),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        ),
        Insight(
            "inlinks_follow_6_10",
            "URLs Between 6 and 10 Follow Inlinks",
            PositiveTrend.UNKNOWN,
            BetweenFilter(field, [6, 10]),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    ]


def get_inlinks_above_below_average_insights():
    #TODO implement this
    #2 ideas for this:
    # - wait for elasticsearch to implement nested queries
    # - implement Insight.compute_value() and by default launch a query
    #   to ElasticSearch. But create a specific implementation for this
    #   insight that launches a query to compute the average number of inlinks
    #   and an other query to get the URLs below this threshold.
    return []


def get_misc_inlinks_insights():
    """Return misc insights related to inlinks.
    :returns: list - list of Insight
    """
    result = []

    field = "inlinks_internal.nb.nofollow.total"
    result.append(
        Insight(
            "inlinks_has_nofollow",
            "URLs with Nofollow Inlinks",
            PositiveTrend.UNKNOWN,
            GtFilter(field, 0),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    )

    field = "inlinks_internal.nb.follow.unique"
    result.append(
        Insight(
            "inlinks_not_strategic_follow",
            "Non Strategic URLs with Follow Inlinks",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter("strategic.is_strategic", False),
                GtFilter(field, 0)
            ]),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    )

    result.append(
        Insight(
            "inlinks_follow_strategic_1",
            "Strategic URLs with only 1 Follow Inlink",
            PositiveTrend.UNKNOWN,
            AndFilter([
                EqFilter("strategic.is_strategic", True),
                EqFilter(field, 1)
            ]),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    )

    result.append(
        Insight(
            "inlinks_not_2xx_follow",
            "Non 2xx URLs with Follow Inlinks",
            PositiveTrend.UNKNOWN,
            AndFilter([
                NotFilter(BetweenFilter("http_code", [200, 299])),
                GtFilter(field, 0)
            ]),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    )
    return result


def get_average_outlinks_insights():
    """Return insights related to internal outlinks averages.
    :returns: list - list of Insight
    """
    return [
        Insight(
            "outlinks_avg",
            "Average Internal Outlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=AvgAggregation("outlinks_internal.nb.unique"),
            data_type=FLOAT_TYPE
        ),
        Insight(
            "outlinks_avg_follow",
            "Average Internal Follow Outlinks",
            PositiveTrend.UNKNOWN,
            metric_agg=AvgAggregation("outlinks_internal.nb.follow.unique"),
            data_type=FLOAT_TYPE
        )
    ]


def get_outlinks_internal_insights():
    """Return insights related to the presence of internal outlinks.
    :returns: list - list of Insight
    """
    result = []
    field = "outlinks_internal.nb.follow.unique"
    result.append(
        Insight(
            "outlinks_internal_follow",
            "URLs with Internal Follow Outlinks",
            PositiveTrend.UNKNOWN,
            GtFilter(field, 0),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    )
    field = "outlinks_internal.nb.nofollow.unique"
    result.append(
        Insight(
            "outlinks_internal_nofollow",
            "URLs with Internal Nofollow Outlinks",
            PositiveTrend.UNKNOWN,
            GtFilter(field, 0),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    )

    return result


def get_outlinks_external_insights():
    """Return insights related to the presence of external outlinks.
    :returns: list - list of Insight
    """
    result = []
    field = "outlinks_external.nb.follow.unique"
    result.append(
        Insight(
            "outlinks_external_follow",
            "URLs with External Follow Outlinks",
            PositiveTrend.UNKNOWN,
            GtFilter(field, 0),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    )
    field = "outlinks_external.nb.nofollow.unique"
    result.append(
        Insight(
            "outlinks_external_nofollow",
            "URLs with External Nofollow Outlinks",
            PositiveTrend.UNKNOWN,
            GtFilter(field, 0),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    )

    return result


def get_misc_outlinks_insights():
    """Return miscinsights related to outlinks.
    :returns: list - list of Insight
    """
    result = []
    field = "outlinks_errors.total_bad_http_codes"
    result.append(
        Insight(
            "outlinks_errors",
            "URLs with Outlinks to non 2xx Status",
            PositiveTrend.DOWN,
            GtFilter(field, 0),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    )

    field = "outlinks_errors.non_strategic.nb"
    result.append(
        Insight(
            "outlinks_not_strategic",
            "Strategic URLs with Outlinks to non Strategic URLs",
            PositiveTrend.DOWN,
            GtFilter(field, 0),
            additional_fields=[field],
            sort_by=DescendingSort(field)
        )
    )
    return result


def get_http_code_is_good_predicate():
    """Helper function that returns a filter that selects
    urls with good http code
    :returns: Filter
    """
    return BetweenFilter("http_code", [200, 299])  # TODO implement http_code_is_code


def get_canonical_insights():
    """Return miscinsights related to outlinks.
    :returns: list - list of Insight
    """
    return [
        Insight(
            "canonical_set",
            "2xx URLs with a Canonical Set",
            PositiveTrend.UNKNOWN,
            AndFilter([
                get_http_code_is_good_predicate(),
                ExistFilter("canonical.to.url_exists")
            ]),
            additional_fields=["canonical.to.url"],
        ),
        Insight(
            "canonical_bad",
            "2xx URLs with a Canonical Set / not Equal",
            PositiveTrend.UNKNOWN,
            OrFilter([
                AndFilter([
                    get_http_code_is_good_predicate(),
                    NotFilter(ExistFilter("canonical.to.url_exists"))
                ]),
                AndFilter([
                    get_http_code_is_good_predicate(),
                    EqFilter("canonical.to.equal", False)
                ])
            ]),
            additional_fields=["canonical.to.url"],
        ),
        Insight(
            "canonical_not_set",
            "2xx URLs with a Canonical not Set",
            PositiveTrend.UNKNOWN,
            AndFilter([
                get_http_code_is_good_predicate(),
                NotFilter(ExistFilter("canonical.to.url_exists"))
            ]),
            additional_fields=["canonical.to.url"]
        ),
        Insight(
            "canonical_not_equal",
            "2xx URLs with a Canonical not Equal",
            PositiveTrend.UNKNOWN,
            AndFilter([
                get_http_code_is_good_predicate(),
                EqFilter("canonical.to.equal", False)
            ]),
            additional_fields=["canonical.to.url"]
        ),
        Insight(
            "canonical_equal",
            "2xx URLs with a Canonical Equal",
            PositiveTrend.UNKNOWN,
            AndFilter([
                get_http_code_is_good_predicate(),
                EqFilter("canonical.to.equal", True)
            ]),
            additional_fields=["canonical.to.url"]
        )
    ]


def get_links_insights():
    result = []
    result.extend(get_average_inlinks_insights())
    result.extend(get_inlinks_sum_insights())
    result.extend(get_outlinks_sum_insights())
    result.extend(get_inlinks_range_insights())
    result.extend(get_inlinks_above_below_average_insights())
    result.extend(get_misc_inlinks_insights())
    result.extend(get_average_outlinks_insights())
    result.extend(get_outlinks_internal_insights())
    result.extend(get_outlinks_external_insights())
    result.extend(get_misc_outlinks_insights())
    result.extend(get_canonical_insights())
    return result

#actual insight definition
insights = get_links_insights()
