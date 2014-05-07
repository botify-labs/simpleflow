from cdf.metadata.url.url_metadata import (
    INT_TYPE, ES_DOC_VALUE, AGG_NUMERICAL
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
        ('bounce_rate', float),
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
    entry = {
        "type": INT_TYPE,
        "settings": {
            ES_DOC_VALUE,
            AGG_NUMERICAL
        }
    }
    for search_engine in organic_sources:
        key = "visits.organic.{}.nb".format(search_engine)
        result[key] = dict(entry)

    for social_network in social_sources:
        key = "visits.social.{}.nb".format(social_network)
        result[key] = dict(entry)
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
        ('bounce_rate', float),
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
            search_engine_dict = {"nb": 0}
            organic[search_engine] = search_engine_dict
        document["visits"]["organic"] = organic

        social = {}
        for social_network in SOCIAL_SOURCES:
            social_dict = {"nb": 0}
            social[social_network] = social_dict
        document["visits"]["social"] = social

    def process_document(self, document, stream):
        _, medium, source, social_network, nb_visits, nb_sessions, bounce_rate, pages_per_session, average_session_duration, percentage_new_sessions, goal_conversion_rate_all = stream
        if social_network and social_network in SOCIAL_SOURCES:
            document['visits']['social'][social_network]['nb'] += nb_visits
        elif medium == 'organic' and source in ORGANIC_SOURCES:
            document['visits'][medium][source]['nb'] = nb_visits
        return
