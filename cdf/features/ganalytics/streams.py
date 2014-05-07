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
        ('pages_per_session', float),
        ('average_session_duration', float),
        ('percentage_new_sessions', float),
        ('goal_conversion_rate_all', float)
   )


def _get_url_document_mapping(organic_sources, social_sources):
    """Helper function to generate the mapping for VisitsStreamDef
    :param organic_sources: the list of organic traffic sources to consider.
                            each traffic source is represented as a string.
    :type organic_sources: list
    :param social_sources: the list of social traffic sources to consider.
                           each traffic source is represented as a string.
    :type social_sources: list
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
        key = "visits.organic.{}.bounce_rate".format(search_engine)
        result[key] = dict(float_entry)

    for social_network in social_sources:
        key = "visits.social.{}.nb".format(social_network)
        result[key] = dict(int_entry)
        key = "visits.social.{}.bounce_rate".format(social_network)
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
        ('pages_per_session', float),
        ('average_session_duration', float),
        ('percentage_new_sessions', float),
        ('goal_conversion_rate_all', float)
    )

    URL_DOCUMENT_MAPPING = _get_url_document_mapping(ORGANIC_SOURCES,
                                                     SOCIAL_SOURCES)

    def pre_process_document(self, document):
        document["visits"] = {}
        organic = {}
        for search_engine in ORGANIC_SOURCES:
            search_engine_dict = {"nb": 0, "sessions": 0, "bounces": 0}
            organic[search_engine] = search_engine_dict
        document["visits"]["organic"] = organic

        social = {}
        for social_network in SOCIAL_SOURCES:
            social_dict = {"nb": 0, "sessions": 0, "bounces": 0}
            social[social_network] = social_dict
        document["visits"]["social"] = social

    def process_document(self, document, stream):
        _, medium, source, social_network, nb_visits, nb_sessions, bounces, pages_per_session, average_session_duration, percentage_new_sessions, goal_conversion_rate_all = stream
        if social_network and social_network in SOCIAL_SOURCES:
            document['visits']['social'][social_network]['nb'] += nb_visits
            document['visits']['social'][social_network]['sessions'] += nb_sessions
            document['visits']['social'][social_network]['bounces'] += bounces
        elif medium == 'organic' and source in ORGANIC_SOURCES:
            document['visits'][medium][source]['nb'] += nb_visits
            document['visits'][medium][source]['sessions'] += nb_sessions
            document['visits'][medium][source]['bounces'] += bounces
        return

    def post_process_document(self, document):
        for search_engine in ORGANIC_SOURCES:
            current_entry = document["visits"]["organic"][search_engine]
            self.compute_metrics(current_entry)
            self.delete_intermediary_metrics(current_entry)

        for social_network in SOCIAL_SOURCES:
            current_entry = document["visits"]["social"][social_network]
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
        bounces = input_dict["bounces"]

        sessions = input_dict["sessions"]
        if sessions != 0:
            bounce_rate = 100 * float(bounces)/float(sessions)
        else:
            bounce_rate = 0.0
        bounce_rate = round(bounce_rate, 2)
        input_dict["bounce_rate"] = bounce_rate

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
        for key in ["bounces", "sessions"]:
            if key in traffic_source_data:
                del traffic_source_data[key]
