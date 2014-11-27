from cdf.core.insights import (
    Insight,
    PositiveTrend,
    strategic_to_compliant_migration_decorator
)
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
            "URLs in Sitemaps and Structure",
            PositiveTrend.UP,
            EqFilter("sitemaps.present", True)
        ),
        #TODO handle sitemaps_urls_only_sitemaps
        Insight(
            "sitemaps_urls_only_structure",
            "URLs not in Sitemaps",
            PositiveTrend.DOWN,
            EqFilter("sitemaps.present", False)
        )
    ]


def get_compliant_sitemap_insights():
    """Return the insights related to the sitemaps and is_strategic field.
    :returns: list - list of Insight
    """
    return [
        Insight(
            "sitemaps_not_strategic",
            "Not Compliant URLs in Sitemaps",
            PositiveTrend.DOWN,
            AndFilter([
                EqFilter("strategic.is_strategic", False),
                EqFilter("sitemaps.present", True)
            ])
        ),
        Insight(
            "sitemaps_strategic",
            "Compliant URLs in Sitemaps",
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
            "URLs in Sitemaps with a Bad HTTP Status Code",
            PositiveTrend.DOWN,
            AndFilter([
                NotFilter(BetweenFilter("http_code", [200, 299])),
                EqFilter("sitemaps.present", True)
            ])
        ),
        Insight(
            "sitemaps_1_follow_inlink",
            "URLs in Sitemaps with only 1 Follow Inlink",
            PositiveTrend.DOWN,
            AndFilter([
                EqFilter("inlinks_internal.nb.follow.unique", 1),
                EqFilter("sitemaps.present", True)
            ])
        ),
        Insight(
            "sitemaps_not_strategic_outlink",
            "URLs in Sitemaps with a not Compliant Outlink",
            PositiveTrend.DOWN,
            AndFilter([
                GtFilter("outlinks_errors.non_strategic.nb.follow.unique", 0),
                EqFilter("sitemaps.present", True)
            ]),
            additional_fields=["outlinks_errors.non_strategic.nb.follow.unique"]
        ),
        Insight(
            "sitemaps_slow_urls",
            "Slow / Slowest URLs in Sitemaps",
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
            "URLs in Sitemaps with a meta no-index",
            PositiveTrend.DOWN,
            AndFilter([
                EqFilter("metadata.robots.noindex", True),
                EqFilter("sitemaps.present", True)
            ])
        ),
    ]


def get_bad_metadata_compliant_sitemap_insights():
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
            "Compliant URLs in Sitemaps with a Bad {}".format(metadata.title()),
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

@strategic_to_compliant_migration_decorator
def get_sitemaps_insights():
    result = []
    result.extend(get_main_sitemap_insights())
    result.extend(get_compliant_sitemap_insights())
    result.extend(get_misc_sitemap_insights())
    result.extend(get_bad_metadata_compliant_sitemap_insights())
    return result


insights = get_sitemaps_insights()
