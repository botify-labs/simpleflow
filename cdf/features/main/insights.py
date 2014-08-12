from cdf.core.insights import Insight
from cdf.query.filter import Predicate


def get_http_code_ranges_insights():
    #insights by http ranges
    result = []
    for range_start in [200, 300, 400, 500]:
        range_name = "{}xx".format(range_start / 100)
        insight = Insight(
            "code:{}".format(range_name),
            "{} Urls".format(range_name),
            {
                "filters": Predicate(
                    "http_code",
                    "between",
                    [range_start, range_start + 99]
                ).to_dict()
            }
        )
        result.append(insight)

    #special range for codes lower than 0
    result.append(
        Insight(
            "code:network_errors",
            "Network Errors",
            {
                "filters": Predicate("http_code", "lt", 0).to_dict()
            }
        )
    )

    return result


def get_http_code_insights():
    #insights by http code
    result = []
    for code in [200, 301, 302, 304, 403, 404, 410]:
        insight = Insight(
            "code:{}".format(code),
            "{} Urls".format(code),
            {
                "filters": Predicate("http_code", "eq", code).to_dict()
            }
        )
        result.append(insight)
    return result


def get_strategic_urls_insights():
    return []
    #FIXME uncomment this when strategic urls have been implemented
#    return [
#        Insight(
#            "strategic:1",
#            "Strategic Urls",
#            {
#                "filters": Predicate("strategic", "eq", True).to_dict()
#            }
#        ),
#        Insight(
#            "strategic:0",
#            "Non Strategic Urls",
#            {
#                "filters": Predicate("strategic", "eq", False).to_dict()
#            }
#        )
#    ]


def get_content_type_insights():
    #insights for content types
    result = []
    for content_type in ["text/html", "text/css"]:
        insight = Insight(
            "content:{}".format(content_type),
            "{} Urls".format(content_type[len("text/"):].upper()),
            {
                "filters": Predicate("content_type", "eq", content_type).to_dict()
            }
        )
        result.append(insight)
    return result


def get_protocol_insights():
    result = []
    for protocol in ["http", "https"]:
        insight = Insight(
            "protocol:{}".format(protocol),
            "{} Urls".format(protocol.upper()),
            {
                "filters": Predicate("protocol", "eq", protocol).to_dict()
            }
        )
        result.append(insight)
    return result


#speed insights
def get_speed_insights():
    field = "delay_last_byte"
    return [
        Insight(
            "speed:fast",
            "Fast Urls",
            {
                "filters": Predicate(field, "lt", 500).to_dict()
            }
        ),
        Insight(
            "speed:medium",
            "Medium Urls",
            {
                "filters": Predicate(field, "between", [500, 999]).to_dict()
            }
        ),
        Insight(
            "speed:slow",
            "Slow Urls",
            {
                "filters": Predicate(field, "between", [1000, 1999]).to_dict()
            }
        ),
        Insight(
            "speed:slowest",
            "Slowest Urls",
            {
                "filters":  Predicate(field, "gte", 2000).to_dict()
            }
        ),
    ]


def get_strategic_urls_speed_insights():
    return []
    #FIXME uncomment when strategic urls have been implemented
#    field = "delay_last_byte"
#    strategic_predicate = Predicate("strategic", "eq", True)
#    return [
#        Insight(
#            "speed:fast_strategic",
#            "Fast Strategic Urls",
#            {
#                "filters": {
#                    "and": [
#                        strategic_predicate.to_dict(),
#                        Predicate(field, "lt", 500).to_dict()
#                    ]
#                }
#            }
#        ),
#        Insight(
#            "speed:medium_strategic",
#            "Medium Strategic Urls",
#            {
#                "filters": {
#                    "and": [
#                        strategic_predicate.to_dict(),
#                        Predicate(field, "between", [500, 999]).to_dict()
#                    ]
#                }
#            }
#        ),
#        Insight(
#            "speed:slow_strategic",
#            "Slow Strategic Urls",
#            {
#                "filters": {
#                    "and": [
#                        strategic_predicate.to_dict(),
#                        Predicate(field, "between", [1000, 1999])
#                    ]
#                }
#            }
#        ),
#        Insight(
#            "speed:slowest_strategic",
#            "Slowest Strategic Urls",
#            {
#                "filters": {
#                    "and": [
#                        strategic_predicate.to_dict(),
#                        Predicate(field, "gte", 2000)
#                    ]
#                }
#            }
#        ),
#    ]


#insights for domain/subdomains
def get_domain_insights():
    www_predicate = Predicate("host", "eq", "www")
    return [
        Insight(
            "domain:www",
            "Urls from www",
            {"filters": www_predicate.to_dict()}
        ),
        Insight(
            "not www",
            "Urls from subdomains",
            {"filters": {"not": www_predicate.to_dict()}}
        )
    ]


def get_average_speed_insights():
    field = "delay_last_byte"
    return [
        Insight(
            "speed:avg",
            "Average Load time (in ms)",
            {
                'aggs': [
                    {
                        'metrics': [
                            {"avg": field}
                        ]
                    }
                ]
            }
        )
    ]
    #TODO"speed:strategic_avg", "Average Load time on strategic urls (in ms)", {"field": "strategic", "value": true}),


def get_average_depth_insights():
    field = "depth"
    return [
        Insight(
            "depth:avg",
            "Average Depth",
            {
                'aggs': [
                    {
                        'metrics': [
                            {"avg": field}
                        ]
                    }
                ]
            }
        )
    ]
    #TODO"depth:strategic_avg", "Average Depth on strategic urls", {"field": "strategic", "value": true}),


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
