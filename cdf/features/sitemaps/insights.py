from cdf.core.insights import Insight, PositiveTrend
from cdf.query.filter import (
    EqFilter,
    GtFilter,
    GteFilter,
    AndFilter,
    OrFilter,
    BetweenFilter,
    NotFilter,
    ExistFilter)
from cdf.query.sort import DescendingSort


def get_main_sitemap_insights():
    """Return the main insights related to the sitemaps.
    :returns: list - list of Insight
    """
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
    """Return the insights related to the sitemaps and is_strategic field.
    :returns: list - list of Insight
    """
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
    """Return misc insights related to sitemaps.
    :returns: list - list of Insight
    """
    return [
        Insight(
            "sitemaps_bad_http_code",
            "URLs in Sitemap with a Bad HTTP Status Code",
            PositiveTrend.DOWN,
            AndFilter([
                NotFilter(BetweenFilter("http_code", [200, 299])),
                EqFilter("sitemaps.present", True)
            ])
        ),
        Insight(
            "sitemaps_1_follow_inlink",
            "URLs in Sitemap with only 1 Follow Inlink",
            PositiveTrend.DOWN,
            AndFilter([
                EqFilter("inlinks_internal.nb.follow.unique", 1),
                EqFilter("sitemaps.present", True)
            ])
        ),
        Insight(
            "sitemaps_not_strategic_outlink",
            "URLs in Sitemap with a not Strategic Outlink",
            PositiveTrend.DOWN,
            AndFilter([
                ExistFilter("outlinks_errors.non_strategic.urls"),
                EqFilter("sitemaps.present", True)
            ])
        ),
        Insight(
            "sitemaps_slow_urls",
            "Slow / Slowest URLs in Sitemap",
            PositiveTrend.DOWN,
            AndFilter([
                GteFilter("delay_last_byte", 1000),
                EqFilter("sitemaps.present", True)
            ]),
            additional_fields=["delay_last_byte"],
            sort_by=DescendingSort("delay_last_byte")
        ),
        Insight(
            "sitemaps_no_index",
            "URLs in Sitemap with a meta no-index",
            PositiveTrend.DOWN,
            AndFilter([
                EqFilter("metadata.robots.noindex", True),
                EqFilter("sitemaps.present", True)
            ])
        ),
    ]


def get_bad_metadata_strategic_sitemap_insights():
    """Return insights related to sitemaps and bad metadata.
    :returns: list - list of Insight
    """
    result = []
    for metadata in ["title", "h1", "description"]:
        additional_fields = [
            "metadata.{}.contents".format(metadata),
            "metadata.{}.nb".format(metadata),
            "metadata.{}.duplicates.nb".format(metadata),
            "metadata.{}.duplicates.urls".format(metadata)
        ]

        insight = Insight(
            "sitemaps_bad_{}".format(metadata),
            "Strategic URLs in Sitemap with a Bad {}".format(metadata.title()),
            PositiveTrend.DOWN,
            #some fields are duplicated
            #but the URL Explorer is not able to display AndFilter -> OrFilter
            OrFilter([
                AndFilter([
                    EqFilter("metadata.{}.nb".format(metadata), 0),
                    EqFilter("strategic.is_strategic", True),
                    EqFilter("sitemaps.present", True)]),
                AndFilter([
                    GtFilter("metadata.{}.duplicates.nb".format(metadata), 0),
                    EqFilter("strategic.is_strategic", True),
                    EqFilter("sitemaps.present", True)])
            ]),
            additional_fields=additional_fields
        )
        result.append(insight)
    return result


def get_sitemaps_insights():
    result = []
    result.extend(get_main_sitemap_insights())
    result.extend(get_strategic_sitemap_insights())
    result.extend(get_misc_sitemap_insights())
    result.extend(get_bad_metadata_strategic_sitemap_insights())
    return result


insights = get_sitemaps_insights()
