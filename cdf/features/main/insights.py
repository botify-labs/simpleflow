from cdf.core.insights import Insight, ExpectedTrend
from cdf.query.filter import (EqFilter,
                              LtFilter,
                              GteFilter,
                              BetweenFilter,
                              NotFilter)
from cdf.query.aggregation import AvgAggregation


def get_http_code_ranges_insights():
    #insights by http ranges
    result = []
    ranges = [
        (200, ExpectedTrend.UP),
        (300, ExpectedTrend.DOWN),
        (400, ExpectedTrend.DOWN),
        (500, ExpectedTrend.DOWN)
    ]

    for range_start, expected_trend in ranges:
        range_name = "{}xx".format(range_start / 100)
        insight = Insight(
            "code_{}".format(range_name),
            "{} Urls".format(range_name),
            expected_trend,
            BetweenFilter("http_code", [range_start, range_start + 99])
        )
        result.append(insight)

    #special range for codes lower than 0
    result.append(
        Insight(
            "code_network_errors",
            "Network Errors",
            LtFilter("http_code", 0)
        )
    )

    return result


def get_http_code_insights():
    #insights by http code
    result = []
    http_codes = [
        (200, ExpectedTrend.UP),
        (301, ExpectedTrend.DOWN),
        (302, ExpectedTrend.DOWN),
        (304, ExpectedTrend.DOWN),
        (403, ExpectedTrend.DOWN),
        (404, ExpectedTrend.DOWN),
        (410, ExpectedTrend.DOWN),
    ]
    for code, expected_trend in http_codes:
        insight = Insight(
            "code_{}".format(code),
            "{} Urls".format(code),
            expected_trend,
            EqFilter("http_code", code)
        )
        result.append(insight)
    return result


def get_strategic_urls_insights():
    return []
    #FIXME uncomment this when strategic urls have been implemented
#    return [
#        Insight(
#            "strategic_1",
#            "Strategic Urls",
#            ExpectedTrend.UP,
#            EqFilter("strategic", True)
#        ),
#        Insight(
#            "strategic_0",
#            "Non Strategic Urls",
#            ExpectedTrend.DOWN,
#            EqFilter("strategic", False)
#        )
#    ]


def get_content_type_insights():
    #insights for content types
    result = []
    for content_type in ["text/html", "text/css"]:
        insight = Insight(
            "content_{}".format(content_type),
            "{} Urls".format(content_type[len("text/"):].upper()),
            ExpectedTrend.UNKNOWN,
            EqFilter("content_type", content_type)
        )
        result.append(insight)
    return result


def get_protocol_insights():
    result = []
    for protocol in ["http", "https"]:
        insight = Insight(
            "protocol_{}".format(protocol),
            "{} Urls".format(protocol.upper()),
            ExpectedTrend.UNKNOWN,
            EqFilter("protocol", protocol)
        )
        result.append(insight)
    return result


#speed insights
def get_speed_insights():
    field = "delay_last_byte"
    return [
        Insight(
            "speed_fast",
            "Fast Urls",
            ExpectedTrend.UP,
            LtFilter(field, 500)
        ),
        Insight(
            "speed_medium",
            "Medium Urls",
            ExpectedTrend.DOWN,
            BetweenFilter(field, [500, 999])
        ),
        Insight(
            "speed_slow",
            "Slow Urls",
            ExpectedTrend.DOWN,
            BetweenFilter(field, [1000, 1999])
        ),
        Insight(
            "speed_slowest",
            "Slowest Urls",
            ExpectedTrend.DOWN,
            GteFilter(field, 2000)
        ),
    ]


def get_strategic_urls_speed_insights():
    return []
    #FIXME uncomment when strategic urls have been implemented
#    field = "delay_last_byte"
#    strategic_predicate = EqFilter("strategic", True)
#    return [
#        Insight(
#            "speed_fast_strategic",
#            "Fast Strategic Urls",
#            ExpectedTrend.UP,
#            AndFilter(
#                [
#                    strategic_predicate,
#                    LtFilter(field, 500)
#                ]
#            )
#        ),
#        Insight(
#            "speed_medium_strategic",
#            "Medium Strategic Urls",
#            ExpectedTrend.DOWN,
#            AndFilter(
#                [
#                    strategic_predicate,
#                    BetweenFilter(field, [500, 999])
#                ]
#            )
#        ),
#        Insight(
#            "speed_slow_strategic",
#            "Slow Strategic Urls",
#            ExpectedTrend.DOWN,
#            AndFilter(
#                [
#                    strategic_predicate,
#                    BetweenFilter(field, [1000, 1999])
#                ]
#            )
#        ),
#        Insight(
#            "speed_slowest_strategic",
#            "Slowest Strategic Urls",
#            ExpectedTrend.DOWN,
#            AndFilter(
#                [
#                    strategic_predicate,
#                    GteFilter(field, 2000)
#                ]
#            )
#        ),
#    ]


#insights for domain/subdomains
def get_domain_insights():
    www_predicate = EqFilter("host", "www")
    return [
        Insight(
            "domain_www",
            "Urls from www",
            ExpectedTrend.UNKNOWN,
            www_predicate
        ),
        Insight(
            "domain_not_www",
            "Urls from subdomains",
            ExpectedTrend.UNKNOWN,
            NotFilter(www_predicate)
        )
    ]


def get_average_speed_insights():
    field = "delay_last_byte"
    return [
        Insight(
            "speed_avg",
            "Average Load time (in ms)",
            ExpectedTrend.DOWN,
            metric_agg=AvgAggregation(field)
        )
    ]
    #TODO"speed_strategic_avg", "Average Load time on strategic urls (in ms)", {"field": "strategic", "value": true}),


def get_average_depth_insights():
    field = "depth"
    return [
        Insight(
            "depth_avg",
            "Average Depth",
            ExpectedTrend.DOWN,
            metric_agg=AvgAggregation(field)
        )
    ]
    #TODO"depth_strategic_avg", "Average Depth on strategic urls", {"field": "strategic", "value": true}),


def get_insights():
    insights = []
    insights.extend(get_http_code_ranges_insights())
    insights.extend(get_http_code_insights())
    insights.extend(get_strategic_urls_insights())
    insights.extend(get_content_type_insights())
    insights.extend(get_protocol_insights())
    insights.extend(get_speed_insights())
    insights.extend(get_strategic_urls_speed_insights())
    insights.extend(get_domain_insights())
    insights.extend(get_average_speed_insights())
    insights.extend(get_average_depth_insights())
    return insights

#actual insight definition
insights = get_insights()
