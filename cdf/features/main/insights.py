from cdf.core.insights import Insight


def get_http_code_ranges_insights():
    #insights by http ranges
    result = []
    for range_start in [200, 300, 400, 500]:
        range_name = "{}xx".format(range_start / 100)
        insight = Insight(
            "code:{}".format(range_name),
            "{} Urls".format(range_name),
            {
                "filters": {
                    "field": "http_code",
                    "predicate": "between",
                    "value": [range_start, range_start + 99]
                }
            }
        )
        result.append(insight)

    #special range for codes lower than 0
    result.append(
        Insight(
            "code:network_errors",
            "Network Errors",
            {
                "filters": {
                    "field": "http_code",
                    "predicate": "lt",
                    "value": 0
                }
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
                "filters": {
                    "field": "http_code",
                    "predicate": "eq",
                    "value": code
                }
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
#                "filters": {
#                    "field": "strategic",
#                    "predicate": "eq",
#                    "value": True
#                }
#            }
#        ),
#        Insight(
#            "strategic:0",
#            "Non Strategic Urls",
#            {
#                "filters": {
#                    "field": "strategic",
#                    "predicate": "eq",
#                    "value": False
#                }
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
                "filters": {
                    "field": "content_type",
                    "predicate": "eq",
                    "value": content_type
                }
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
                "filters": {
                    "field": "protocol",
                    "predicate": "eq",
                    "value": protocol}
            }
        )
        result.append(insight)
    return result


#speed insights
def get_speed_insights():
    return [
        Insight(
            "speed:fast",
            "Fast Urls",
            {
                "filters": {
                    "field": "delay_last_byte",
                    "value": 500,
                    "predicate": "lt"}
            }
        ),
        Insight(
            "speed:medium",
            "Medium Urls",
            {
                "filters": {
                    "field": "delay_last_byte",
                    "value": [500, 999],
                    "predicate": "between"}
            }
        ),
        Insight(
            "speed:slow",
            "Slow Urls",
            {
                "filters": {
                    "field": "delay_last_byte",
                    "value": [1000, 1999],
                    "predicate": "between"}
            }
        ),
        Insight(
            "speed:slowest",
            "Slowest Urls",
            {
                "filters":  {
                    "field": "delay_last_byte",
                    "value": 2000,
                    "predicate": "gte"}
            }
        ),
    ]


def get_strategic_urls_speed_insights():
    return []
    #FIXME uncomment when strategic urls have been implemented
#    return [
#        Insight(
#            "speed:fast_strategic",
#            "Fast Strategic Urls",
#            {
#                "filters": {
#                    "and": [
#                        {
#                            "field": "strategic",
#                            "predicate": "eq",
#                            "value": True
#                        },
#                        {
#                            "field": "delay_last_byte",
#                            "value": 500,
#                            "predicate": "lt"
#                        }
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
#                        {
#                            "field": "strategic",
#                            "predicate": "eq",
#                            "value": True
#                        },
#                        {
#                            "field": "delay_last_byte",
#                            "value": [500, 999],
#                            "predicate": "between"
#                        }
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
#                        {
#                            "field": "strategic",
#                            "predicate": "eq",
#                            "value": True
#                        },
#                        {
#                            "field": "delay_last_byte",
#                            "value": [1000, 1999],
#                            "predicate": "between"
#                        }
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
#                        {
#                            "field": "strategic",
#                            "predicate": "eq",
#                            "value": True
#                        },
#                        {
#                            "field": "delay_last_byte",
#                            "value": 2000,
#                            "predicate": "gte"
#                        }
#                    ]
#                }
#            }
#        ),
#    ]


#insights for domain/subdomains
def get_domain_insights():
    return [
        Insight(
            "domain:www",
            "Urls from www",
            {"filters": {"field": "host", "predicate": "eq", "value": "www"}}
        ),
        Insight(
            "not www",
            "Urls from subdomains",
            {"filters": {"not": {"field": "host", "predicate": "eq", "value": "www"}}}
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
