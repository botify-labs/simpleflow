from cdf.core.insights import Insight, PositiveTrend
from cdf.query.filter import (
    EqFilter,
    GtFilter,
    AndFilter,
    OrFilter,
    BetweenFilter,
    NotFilter,
    ExistFilter)
from cdf.query.sort import DescendingSort
from cdf.query.aggregation import AvgAggregation, SumAggregation

def get_main_sitemap_insights():
    return [
        Insight(
            "sitemaps_urls_common",
            "URLs in Sitemap and Structure",
            PositiveTrend.UP,
            EqFilter("sitemaps.present", True)
        ),
        #TODO handle sitemaps_urls_only_sitemaps
        Insight(
            "sitemaps_urls_only_structure",
            "URLs only in Structure",
            PositiveTrend.DOWN,
            EqFilter("sitemaps.present", False)
        )
    ]


def get_strategic_sitemap_insights():
    return [
        Insight(
            "sitemaps_not_strategic",
            "Not Strategic URLs in Sitemap",
            PositiveTrend.DOWN,
            AndFilter([
                EqFilter("strategic.is_strategic", False),
                EqFilter("sitemaps.present", True)
            ])
        ),
        Insight(
            "sitemaps_strategic",
            "Strategic URLs in Sitemap",
            PositiveTrend.UP,
            AndFilter([
                EqFilter("strategic.is_strategic", True),
                EqFilter("sitemaps.present", True)
            ])
        )
    ]


def get_misc_sitemap_insights():
    return [
        Insight(
            "sitemaps_bad_http_code",
            "URLs in Sitemap with a Bad HTTP Code",
            PositiveTrend.DOWN,
            AndFilter([
                NotFilter(BetweenFilter("http_code", [200, 299])),
                EqFilter("sitemaps.present", True)
            ])
        ),
        Insight(
            "sitemaps_1_follow_link",
            "URLs in Sitemap with only 1 Follow Link",
            PositiveTrend.DOWN,
            AndFilter([
                EqFilter("strategic.is_strategic"),
                EqFilter("sitemaps.present", True)
            ])
        )
    ]


insights = []
insights.extend(get_main_sitemap_insights())
insights.extend(get_strategic_sitemap_insights())
