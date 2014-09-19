from cdf.metadata.url.url_metadata import FLOAT_TYPE
from cdf.query.constants import RENDERING
from cdf.core.insights import Insight, PositiveTrend
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
        (200, PositiveTrend.UP),
        (300, PositiveTrend.DOWN),
        (400, PositiveTrend.DOWN),
        (500, PositiveTrend.DOWN)
    ]

    for range_start, positive_trend in ranges:
        range_name = "{}xx".format(range_start / 100)
        insight = Insight(
            "code_{}".format(range_name),
            "{} Urls".format(range_name),
            positive_trend,
            BetweenFilter("http_code", [range_start, range_start + 99])
        )
        result.append(insight)

    #special range for codes lower than 0
    result.append(
        Insight(
            "code_network_errors",
            "Network Errors",
            PositiveTrend.DOWN,
            LtFilter("http_code", 0)
        )
    )

    return result


def get_http_code_insights():
    #insights by http code
    result = []
    http_codes = [
        (200, PositiveTrend.UP),
        (301, PositiveTrend.DOWN),
        (302, PositiveTrend.DOWN),
        (304, PositiveTrend.DOWN),
        (403, PositiveTrend.DOWN),
        (404, PositiveTrend.DOWN),
        (410, PositiveTrend.DOWN),
    ]
    for code, positive_trend in http_codes:
        insight = Insight(
            "code_{}".format(code),
            "{} Urls".format(code),
            positive_trend,
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
#            PositiveTrend.UP,
#            EqFilter("strategic", True)
#        ),
#        Insight(
#            "strategic_0",
#            "Non Strategic Urls",
#            PositiveTrend.DOWN,
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
            PositiveTrend.UNKNOWN,
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
            PositiveTrend.UNKNOWN,
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
            PositiveTrend.UP,
            LtFilter(field, 500),
            field_type=RENDERING.TIME_MILLISEC.value  #the param is a string so we need to use the enum value
        ),
        Insight(
            "speed_medium",
            "Medium Urls",
            PositiveTrend.DOWN,
            BetweenFilter(field, [500, 999]),
            field_type=RENDERING.TIME_MILLISEC.value  #the param is a string so we need to use the enum value
        ),
        Insight(
            "speed_slow",
            "Slow Urls",
            PositiveTrend.DOWN,
            BetweenFilter(field, [1000, 1999]),
            field_type=RENDERING.TIME_MILLISEC.value  #the param is a string so we need to use the enum value
        ),
        Insight(
            "speed_slowest",
            "Slowest Urls",
            PositiveTrend.DOWN,
            GteFilter(field, 2000),
            field_type=RENDERING.TIME_MILLISEC.value  #the param is a string so we need to use the enum value
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
#            PositiveTrend.UP,
#            AndFilter(
#                [
#                    strategic_predicate,
#                    LtFilter(field, 500),
#                    field_type=RENDERING.TIME_MILLISEC.value  #the param is a string so we need to use the enum value

#                ]
#            )
#        ),
#        Insight(
#            "speed_medium_strategic",
#            "Medium Strategic Urls",
#            PositiveTrend.DOWN,
#            AndFilter(
#                [
#                    strategic_predicate,
#                    BetweenFilter(field, [500, 999]),
#                    field_type=RENDERING.TIME_MILLISEC.value  #the param is a string so we need to use the enum value

#                ]
#            )
#        ),
#        Insight(
#            "speed_slow_strategic",
#            "Slow Strategic Urls",
#            PositiveTrend.DOWN,
#            AndFilter(
#                [
#                    strategic_predicate,
#                    BetweenFilter(field, [1000, 1999]),
#                    field_type=RENDERING.TIME_MILLISEC.value  #the param is a string so we need to use the enum value

#                ]
#            )
#        ),
#        Insight(
#            "speed_slowest_strategic",
#            "Slowest Strategic Urls",
#            PositiveTrend.DOWN,
#            AndFilter(
#                [
#                    strategic_predicate,
#                    GteFilter(field, 2000),
#                    field_type=RENDERING.TIME_MILLISEC.value  #the param is a string so we need to use the enum value

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
            PositiveTrend.UNKNOWN,
            www_predicate
        ),
        Insight(
            "domain_not_www",
            "Urls from subdomains",
            PositiveTrend.UNKNOWN,
            NotFilter(www_predicate)
        )
    ]


def get_average_speed_insights():
    field = "delay_last_byte"
    return [
        Insight(
            "speed_avg",
            "Average Load time (in ms)",
            PositiveTrend.DOWN,
            metric_agg=AvgAggregation(field),
            field_type=RENDERING.TIME_MILLISEC.value  #the param is a string so we need to use the enum value
        )
    ]
    #TODO"speed_strategic_avg", "Average Load time on strategic urls (in ms)", {"field": "strategic", "value": true}),


def get_average_depth_insights():
    field = "depth"
    return [
        Insight(
            "depth_avg",
            "Average Depth",
            PositiveTrend.DOWN,
            metric_agg=AvgAggregation(field),
            field_type=FLOAT_TYPE
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
