from cdf.metadata.url.url_metadata import (
    INT_TYPE, FLOAT_TYPE, ES_DOC_VALUE,
    AGG_NUMERICAL, DIFF_QUANTITATIVE
)
from cdf.core.features import StreamDefBase
from .settings import ORGANIC_SOURCES, SOCIAL_SOURCES
from .metrics import compute_average_value, compute_percentage
from cdf.core.metadata.constants import RENDERING


class RawVisitsStreamDef(StreamDefBase):
    FILE = 'analytics_raw_data'
    HEADERS = (
        ('url', str),
        ('medium', str),
        ('source', str),
        ('social_network', lambda i: i.lower() if i != '(not set)' else None),
        ('nb', int),
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
    if len(ORGANIC_SOURCES) > 0:
        yield "organic", "all"
        for search_engine in ORGANIC_SOURCES:
            yield "organic", search_engine

    if len(SOCIAL_SOURCES) > 0:
        yield "social", "all"
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
    :returns: dict
    """
    result = {}

    if len(organic_sources) > 0:
        _update_document_mapping(result,
                                 "organic",
                                 organic_sources,
                                 metrics)

    if len(social_sources) > 0:
        _update_document_mapping(result,
                                 "social",
                                 social_sources,
                                 metrics)
    return result


def _update_document_mapping(mapping, medium, sources, metrics):
    """Helper function to update a mapping for VisitsStreamDef for a given
    medium.
    :param mapping: the mapping to update
    :type mapping: dict
    :param medium: the medium to use to update the mapping
    :type medium: str
    :param sources: the list of sources to consider
                    each traffic source is represented as a string.
    :type sources: list
    :param metrics: the list of metrics to be included in the mapping,
                    in addition to the number of visits which is always
                    in the mapping;
                    It is given as a list of strings.
    :type metrics: list
    """
    sources += ('all', )

    # Iterate over sources
    for source in sources:
        # Id source == "all", the target is "organic" or "social"
        if source == "all":
            source_target = "All {} Traffic".format(medium.capitalize())
        # Otherwise, the target is the platform name (twitter, faceboook, google, etc..)
        else:
            source_target = source.capitalize()

        key_prefix = "visits.{}.{}".format(medium, source)
        for i, metric in enumerate(metrics):
            field_name, _, _, verbose_name, data_type, flag = metric
            key = "{}.{}".format(key_prefix, field_name)
            mapping[key] = {
                "type": data_type,
                "settings": {
                    ES_DOC_VALUE,
                    AGG_NUMERICAL,
                    DIFF_QUANTITATIVE
                },
                "verbose_name": verbose_name.format(source=source_target.title()),
                "group": key_prefix,
            }
            if flag:
                mapping[key]["settings"].add(flag)


class VisitsStreamDef(StreamDefBase):
    FILE = 'analytics_data'
    HEADERS = (
        ('id', int),
        ('medium', str),
        ('source', str),
        ('social_network', str),
        ('nb', int),
        ('bounces', int),
        ('page_views', int),
        ('session_duration', float),
        ('new_users', int),
        ('goal_completions_all', int)
    )

    #defines a list of calculated metric definitions
    #a calculated metric is a metric computed from raw metrics
    #each calculated metric is defined by a 6-tuple:
    #  - (calculated metric name, f, raw_metric_name, verbose_name, type, rendering_flag)
    # - if metric name doensn't need any computation, set it to None
    # - if you don't need to set a specific rendering, set it to None
    # such that calculted metric = f(raw_metric, nb_sessions)
    _METRICS = [
        ("nb", None, "nb", "No. of Visits for {source}", INT_TYPE, None),
        ("bounce_rate", compute_percentage, "bounces", "Bounce Rate for {source}", FLOAT_TYPE, RENDERING.PERCENT),
        ("pages_per_session", compute_average_value, "page_views", "Pages per Session for {source}", FLOAT_TYPE, None),
        ("average_session_duration", compute_average_value, "session_duration", "Session Duration for {source}", FLOAT_TYPE, RENDERING.TIME_SEC),
        ("percentage_new_sessions", compute_percentage, "new_users", "% of New Sessions for {source}", FLOAT_TYPE, RENDERING.PERCENT),
        ("goal_conversion_rate_all", compute_percentage, "goal_completions_all", "Goal Conversion Rate for {source}", FLOAT_TYPE, RENDERING.PERCENT)
    ]

    _RAW_METRICS = [
        "nb",
        "bounces",
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
        visit_medium, visit_source = self.get_visit_medium_source(stream)
        if visit_medium != "organic" and visit_medium != "social":
            return

        all_entry = document['visits'][visit_medium]["all"]
        self.update_entry(all_entry, stream)

        if not self.consider_source(stream):
            return

        #update the field corresponding to the current source
        current_entry = document['visits'][visit_medium][visit_source]
        self.update_entry(current_entry, stream)

    def update_entry(self, document_entry, stream_line):
        """Update a document entry with data from a stream line
        :param document_entry: the document entry to update
        :type document_entry: dict
        :param stream_line: the stream line containing the data
        :type stream_line: list
        """
        for metric in VisitsStreamDef._RAW_METRICS:
            metric_index = self.field_idx(metric)
            document_entry[metric] += stream_line[metric_index]

    def consider_source(self, stream_line):
        """Decides whether or not the source of a stream entry should be
        considered"""
        medium = stream_line[self.field_idx("medium")]
        source = stream_line[self.field_idx("source")]
        social_network = stream_line[self.field_idx("social_network")]

        result = False
        if social_network and social_network in SOCIAL_SOURCES:
            result = True
        elif medium == 'organic' and source in ORGANIC_SOURCES:
            result = True
        return result

    def get_visit_medium_source(self, stream_line):
        """Return a tuple (medium, source) for a given stream line
        The medium/source are slightly different from the corresponding
        google analytics dimensions"""
        medium = stream_line[self.field_idx("medium")]
        source = stream_line[self.field_idx("source")]
        social_network = stream_line[self.field_idx("social_network")]

        visit_medium, visit_source = (None, None)
        if social_network and social_network != "None":
            #according to Google Analytics not all visits from
            #social networks are social visits
            #However we choose to keep things simple
            #and to set the medium to social
            visit_medium = "social"
            #the social network is not really a source
            #(for instance the source for Twitter might be t.co)
            #but to keep things simple
            #we abusively assign social network to source
            visit_source = social_network
        elif medium == 'organic':
            visit_medium = "organic"
            visit_source = source
        return (visit_medium, visit_source)

    def post_process_document(self, document):
        for medium, source in _iterate_sources():
            current_entry = document["visits"][medium][source]
            self.compute_calculated_metrics(current_entry)
            self.delete_intermediary_metrics(current_entry)

    def compute_calculated_metrics(self, input_dict):
        """Compute some metrics for a traffic sources.
        Some metric can only be computed as a postprocessing.
        For instance to compute the bounce rate we need to have the full number
        of bounces and the full number of sessions.
        :param traffic_source_data: a dict that contains data about a traffic
                                    source
        :type traffic_source_data: dict
        """
        sessions = input_dict["nb"]
        l = filter(lambda i: i[1] is not None, VisitsStreamDef._METRICS)
        for calculated_metric_name, averaging_function, raw_metric_name, _, _, _ in l:
            raw_metric = input_dict[raw_metric_name]
            input_dict[calculated_metric_name] = averaging_function(raw_metric,
                                                                    sessions)

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
        raw_metrics_to_delete = [
            metric for metric in VisitsStreamDef._RAW_METRICS if
            metric != "nb"
        ]
        for key in raw_metrics_to_delete:
            if key in traffic_source_data:
                del traffic_source_data[key]

