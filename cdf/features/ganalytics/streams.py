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

    metrics = ["bounce_rate", "pages_per_session"]
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
        ('average_session_duration', float),
        ('percentage_new_sessions', float),
        ('goal_conversion_rate_all', float)
    )

    URL_DOCUMENT_MAPPING = _get_url_document_mapping(ORGANIC_SOURCES,
                                                     SOCIAL_SOURCES)

    def pre_process_document(self, document):
        document["visits"] = {}
        organic = {}
        metrics = ["nb", "sessions", "bounces", "page_views"]
        for search_engine in ORGANIC_SOURCES:
            search_engine_dict = {metric: 0 for metric in metrics}
            organic[search_engine] = search_engine_dict
        document["visits"]["organic"] = organic

        social = {}
        for social_network in SOCIAL_SOURCES:
            social_dict = {metric: 0 for metric in metrics}
            social[social_network] = social_dict
        document["visits"]["social"] = social

    def process_document(self, document, stream):
        _, medium, source, social_network, nb_visits, nb_sessions, bounces, page_views, average_session_duration, percentage_new_sessions, goal_conversion_rate_all = stream
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
            document['visits'][visit_type][visit_source]['nb'] += nb_visits
            document['visits'][visit_type][visit_source]['sessions'] += nb_sessions
            document['visits'][visit_type][visit_source]['bounces'] += bounces
            document['visits'][visit_type][visit_source]['page_views'] += page_views

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
        sessions = input_dict["sessions"]

        bounces = input_dict["bounces"]
        input_dict["bounce_rate"] = self.compute_bounce_rate(bounces, sessions)

        page_views = input_dict["page_views"]
        input_dict["pages_per_session"] = self.compute_pages_per_session(page_views,
                                                                         sessions)

    def compute_bounce_rate(self, bounces, sessions):
        """Compute the bounce rate.
        :param bounces: the number of bounces
                        (sessions with only one page)
        :type bounces: int
        :param sessions: the number of sessions
        :type sessions: int
        :returns: float
        """
        if sessions != 0:
            bounce_rate = 100 * float(bounces)/float(sessions)
        else:
            bounce_rate = 0.0
        bounce_rate = round(bounce_rate, 2)
        return bounce_rate

    def compute_pages_per_session(self, page_views, sessions):
        """Compute the number of pages per sessions.
        :param bounces: the total number of page_views
        :type bounces: int
        :param sessions: the number of sessions
        :type sessions: int
        :returns: float
        """
        if sessions != 0:
            pages_per_session = float(page_views)/float(sessions)
        else:
            pages_per_session = 0.0
        pages_per_session = round(pages_per_session, 2)
        return pages_per_session

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
        for key in ["bounces", "sessions", "page_views"]:
            if key in traffic_source_data:
                del traffic_source_data[key]
