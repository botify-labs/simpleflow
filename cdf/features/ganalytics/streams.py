from cdf.metadata.url.url_metadata import (
    INT_TYPE, FLOAT_TYPE, ES_DOC_VALUE, AGG_NUMERICAL
)
from cdf.core.features import StreamDefBase
from .settings import ORGANIC_SOURCES, SOCIAL_SOURCES


class RawVisitsStreamDef(StreamDefBase):
    FILE = 'analytics_raw_data'
    HEADERS = (
        ('url', str),
        ('medium', str),
        ('source', str),
        ('social_network', lambda i: i.lower() if i != '(not set)' else None),
        ('nb_visits', int),
        ('nb_sessions', int),
        ('bounces', int),
        ('page_views', int),
        ('session_duration', float),
        ('new_users', int),
        ('goal_completions_all', int)
   )


def _iterate_sources():
    """Iterate over the considered traffic sources.
    Generate tuples (medium, source) for instance
    ('organic', 'google') or ('social', 'facebook')
    """
    for search_engine in ORGANIC_SOURCES:
        yield "organic", search_engine
    for social_network in SOCIAL_SOURCES:
        yield "social", social_network


def _get_url_document_mapping(organic_sources, social_sources, metrics):
    """Helper function to generate the mapping for VisitsStreamDef
    :param organic_sources: the list of organic traffic sources to consider.
                            each traffic source is represented as a string.
    :type organic_sources: list
    :param social_sources: the list of social traffic sources to consider.
                           each traffic source is represented as a string.
    :type social_sources: list
    :param metrics: the list of metrics to be included in the mapping,
                    in addition to the number of visits which is always
                    in the mapping;
                    It is given as a list of strings.
    :type metrics: list
    """
    result = {}
    int_entry = {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    }
    float_entry = {
        "type": FLOAT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    }

    for search_engine in organic_sources:
        key = "visits.organic.{}.nb".format(search_engine)
        result[key] = dict(int_entry)
        for metric in metrics:
            key = "visits.organic.{}.{}".format(search_engine, metric)
            result[key] = dict(float_entry)

    for social_network in social_sources:
        key = "visits.social.{}.nb".format(social_network)
        result[key] = dict(int_entry)
        for metric in metrics:
            key = "visits.social.{}.{}".format(social_network, metric)
            result[key] = dict(float_entry)
    return result


class VisitsStreamDef(StreamDefBase):
    FILE = 'analytics_data'
    HEADERS = (
        ('id', int),
        ('medium', str),
        ('source', str),
        ('social_network', str),
        ('nb_visits', int),
        ('nb_sessions', int),
        ('bounces', int),
        ('page_views', int),
        ('session_duration', float),
        ('new_users', int),
        ('goal_completions_all', int)
    )

    _METRICS = [
        "bounce_rate",
        "pages_per_session",
        "average_session_duration",
        "percentage_new_sessions",
        "goal_conversion_rate_all"
    ]

    _RAW_METRICS = [
        "nb",
        "sessions",
        "bounces",
        "page_views",
        "session_duration",
        "new_users",
        "goal_completions_all"
    ]

    # metrics that are stored only for intermediary computation and should
    # be removed from the final document
    _RAW_METRICS_TO_DELETE = [
        "bounces",
        "sessions",
        "page_views",
        "session_duration",
        "new_users",
        "goal_completions_all"
    ]

    URL_DOCUMENT_MAPPING = _get_url_document_mapping(ORGANIC_SOURCES,
                                                     SOCIAL_SOURCES,
                                                     _METRICS)

    def pre_process_document(self, document):
        document["visits"] = {}
        document["visits"]["organic"] = {}
        document["visits"]["social"] = {}

        for medium, source in _iterate_sources():
            entry = {metric: 0 for metric in VisitsStreamDef._RAW_METRICS}
            document["visits"][medium][source] = entry

    def process_document(self, document, stream):
        entry_description = {
            "id": 0,
            "medium": 1,
            "source": 2,
            "social_network": 3,
            "nb": 4,  # nb visits
            "sessions": 5,
            "bounces": 6,
            "page_views": 7,
            "session_duration": 8,
            "new_users": 9,
            "goal_completions_all": 10
        }
        _, medium, source, social_network, _, _, _, _, _, _, _ = stream
        update_document = False
        if social_network and social_network in SOCIAL_SOURCES:
            update_document = True
            visit_type = "social"
            visit_source = social_network
        elif medium == 'organic' and source in ORGANIC_SOURCES:
            update_document = True
            visit_type = "organic"
            visit_source = source

        if update_document:
            current_entry = document['visits'][visit_type][visit_source]
            for metric in VisitsStreamDef._RAW_METRICS:
                metric_index = entry_description[metric]
                current_entry[metric] += stream[metric_index]

        return

    def post_process_document(self, document):
        for medium, source in _iterate_sources():
            current_entry = document["visits"][medium][source]
            self.compute_metrics(current_entry)
            self.delete_intermediary_metrics(current_entry)

    def compute_metrics(self, input_dict):
        """Compute some metrics for a traffic sources.
        Some metric can only be computed as a postprocessing.
        For instance to compute the bounce rate we need to have the full number
        of bounces and the full number of sessions.
        :param traffic_source_data: a dict that contains data about a traffic
                                    source
        :type traffic_source_data: dict
        """
        sessions = input_dict["sessions"]
        l = [
            ("bounces", self.compute_percentage, "bounce_rate"),
            ("page_views", self.compute_average_value, "pages_per_session"),
            ("session_duration", self.compute_average_value, "average_session_duration"),
            ("new_users", self.compute_percentage, "percentage_new_sessions"),
            ("goal_completions_all", self.compute_percentage, "goal_conversion_rate_all")
        ]
        for raw_metric_name, averaging_function, average_metric_name in l:
            raw_metric = input_dict[raw_metric_name]
            input_dict[average_metric_name] = averaging_function(raw_metric,
                                                                 sessions)

    def compute_average_value(self, sum_values, total_sessions):
        """Compute the average of a metric.
        If the number of sessions is null, returns 0.
        The returned result is rounded to two decimals
        :param sum_values: the sum of the values of the considered metric
        :value sum_values: int
        :param total_sessions: the total number of sessions
        :param total_sessions: int
        :returns: float
        """
        if total_sessions != 0:
            result = float(sum_values)/float(total_sessions)
        else:
            result = 0.0
        result = round(result, 2)
        return result

    def compute_percentage(self, concerned_sessions, total_sessions):
        """Compute the percentage of sessions concerned by a property.
        If the number of sessions is null, returns 0.
        The returned result is rounded to two decimals
        :param concerned_session: the number of concerned sessions
        :value sum_values: int
        :param total_sessions: the toal number of sessions
        :param total_sessions: int
        :returns: float
        """
        if total_sessions != 0:
            result = 100 * float(concerned_sessions)/float(total_sessions)
        else:
            result = 0.0
        result = round(result, 2)
        return result

    def delete_intermediary_metrics(self, traffic_source_data):
        """Deletes entries from a dict representing a traffic source
        that will not be exported to the final document.
        For instances "bounces" is an entry which purpose is only to compute
        the bounce rate, it should be deleted from the dict
        once the bounce_rate has computed.
        :param traffic_source_dict: a dict that contains data about a traffic
                                    source
        :type traffic_source_dict: dict:
        """
        for key in VisitsStreamDef._RAW_METRICS_TO_DELETE:
            if key in traffic_source_data:
                del traffic_source_data[key]
