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

    metrics = [
        "bounce_rate",
        "pages_per_session",
        "average_session_duration",
        "percentage_new_sessions",
        "goal_conversion_rate_all"
    ]
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

    URL_DOCUMENT_MAPPING = _get_url_document_mapping(ORGANIC_SOURCES,
                                                     SOCIAL_SOURCES)

    def pre_process_document(self, document):
        document["visits"] = {}
        organic = {}
        metrics = [
            "nb",
            "sessions",
            "bounces",
            "page_views",
            "session_duration",
            "new_users",
            "goal_completions_all"
        ]
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
        _, medium, source, social_network, nb_visits, nb_sessions, bounces, page_views, session_duration, new_users, goal_completions_all = stream
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

            current_entry['nb'] += nb_visits
            current_entry['sessions'] += nb_sessions
            current_entry['bounces'] += bounces
            current_entry['page_views'] += page_views
            current_entry['session_duration'] += session_duration
            current_entry['new_users'] += new_users
            current_entry['goal_completions_all'] += goal_completions_all

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

        session_duration = input_dict["session_duration"]
        average_session_duration = self.compute_average_session_duration(
            session_duration,
            sessions)
        input_dict["average_session_duration"] = average_session_duration

        new_users = input_dict["new_users"]
        percentage_new_sessions = self.compute_percentage_new_sessions(
            new_users,
            sessions)
        input_dict["percentage_new_sessions"] = percentage_new_sessions

        goal_completions_all = input_dict["goal_completions_all"]
        goal_conversion_rate_all = self.compute_goal_conversion_rate(
            goal_completions_all,
            sessions)
        input_dict["goal_conversion_rate_all"] = goal_conversion_rate_all

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
        :param page_views: the total number of page_views
        :type page_views: int
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

    def compute_average_session_duration(self, session_duration, sessions):
        """Compute the average session duration (in seconds)
        :param session_duration: the total session duration
        :type session_duration: int
        :param sessions: the number of sessions
        :type sessions: int
        :returns: float
        """
        if sessions != 0:
            average_session_duration = float(session_duration)/float(sessions)
        else:
            average_session_duration = 0.0
        average_session_duration = round(average_session_duration, 2)
        return average_session_duration

    def compute_percentage_new_sessions(self, new_users, sessions):
        """Compute the percentage of new sessions
        :param new_users: the total number of new users
                          (corresponds to "ga:newUsers" metric)
        :type new_session: int
        :param sessions: the number of sessions
        :type sessions: int
        :returns: float
        """
        if sessions != 0:
            percentage_new_sessions = 100 * float(new_users)/float(sessions)
        else:
            percentage_new_sessions = 0.0
        percentage_new_sessions = round(percentage_new_sessions, 2)
        return percentage_new_sessions

    def compute_goal_conversion_rate(self, goal_completions, sessions):
        """Compute the goal conversion rate
        :param goal_completions: the total number of goal completions
                                 for a given goal
        :type goal_completions: int
        :param sessions: the number of sessions
        :type sessions: int
        :returns: float
        """
        if sessions != 0:
            goal_conversion_rate = 100 * float(goal_completions)/float(sessions)
        else:
            goal_conversion_rate = 0.0
        goal_conversion_rate = round(goal_conversion_rate, 2)
        return goal_conversion_rate

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
        intermediary_metrics = [
            "bounces",
            "sessions",
            "page_views",
            "session_duration",
            "new_users",
            "goal_completions_all"
        ]
        for key in intermediary_metrics:
            if key in traffic_source_data:
                del traffic_source_data[key]
