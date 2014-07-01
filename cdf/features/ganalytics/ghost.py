import os
import json
import heapq
from collections import Counter

from cdf.utils.dict import deep_dict
from cdf.features.ganalytics.settings import ORGANIC_SOURCES, SOCIAL_SOURCES
from cdf.features.ganalytics.streams import (RawVisitsStreamDef,
                                             _iterate_sources)


def update_session_count(ghost_pages, medium, source, social_network, nb_sessions):
    """Update the counter that stores the number of ghost page sessions per medium/source
    :param ghost_pages: a Counter medium/source -> ghost_pages where ghost_pages is
                        a dict url -> nb sessions
                        that stores the ghost pages.
                        It will be updated by the function.
                        Keys have the form "organic.all", "organic.google", etc.
    :type ghost_pages: Counter
    :param medium: the traffic medium of the current entry
    :type medium: str
    :param source: the traffic source of the current entry
    :type source: str
    :param social_network: the social network of the current entry
    :type social_network: str
    :param nb_sessions: the number of sessions of the current entry
    :type nb_sessions: int
    :param entry: the entry to use to update the ghost pages.
                  this is a RawVisitsStreamDef entry
    :type entry: list
    """
    for medium_source in get_medium_sources(medium, source, social_network):
        ghost_pages[medium_source] += nb_sessions


def update_top_ghost_pages(top_ghost_pages, nb_top_ghost_pages,
                           url, session_count):
    """Update the top ghost pages with the sessions from one url
    :param top_ghost_pages: a dict medium/source -> top_source_ghost_pages
                            with top_ghost_pages a heap of tuples
                            (nb_sessions, url) that stores the top ghost pages
                            for the current source
    :type top_ghost_pages: dict
    :param nb_top_ghost_pages: the number of ghost pages to keep for each
                               traffic source
    :type nb_top_ghost_pages: int
    :param url: the url to update top ghost pages with
    :type url: str
    :param session_count: a list of tuples (source, nb_sessions)
                          that represents all the sessions for the input url
    :type session_count: int
    """
    #update the top ghost pages for this url
    for medium_source, nb_sessions in session_count.iteritems():
        if medium_source not in top_ghost_pages:
            top_ghost_pages[medium_source] = []

        #update each source
        crt_ghost_pages_heap = top_ghost_pages[medium_source]

        if len(crt_ghost_pages_heap) < nb_top_ghost_pages:
            heapq.heappush(crt_ghost_pages_heap, (nb_sessions, url))
        else:
            heapq.heappushpop(crt_ghost_pages_heap, (nb_sessions, url))


def get_medium_sources(medium, source, social_network):
    """Returns a list of traffic medium/sources the input entry contributes to.
    For instance a visit from google counts as an organic visit but also
    as a google visit.
    Thus this function will return ["organic", "visit"]
    :param medium: the traffic medium to consider.
    :type medium: str
    :param source: the traffic source to consider.
    :type source: str
    :param social_network: the social network to consider
    :type social_network: str
    """
    result = []
    if medium == "organic":
        result.append("organic.all")
        if source in ORGANIC_SOURCES:
            result.append("organic.{}".format(source))

    if social_network is not None:
        result.append("social.all")
        if social_network in SOCIAL_SOURCES:
            result.append("social.{}".format(social_network))
    return result


def build_ghost_counts_dict(ghost_pages_session_count, ghost_pages_url_count):
    """Build a dict that stores counts about ghost pages.
    It mixes the ghost url counts with the number of sessions for ghost pages.
    The result dict has the form
    {
        "organic.all.nb_visits": 10,
        "organic.all.nb_urls: 5
        "organic.google.nb_visits": ...
    }
    :param ghost_pages_session_count: a dict medium/source -> nb sessions
                                      keys have the form "organic.all",
                                      "social.facebook".
    :type ghost_pages_session_count: dict
    :param ghost_pages_url_count: a dict medium/source -> nb urls
                                      keys have the form "organic.all",
                                      "social.facebook".
    :type ghost_pages_url_count: dict
    :returns: dict
    """
    result = {}
    for medium_source, count in ghost_pages_session_count.iteritems():
        key = "{}.nb_visits".format(medium_source)
        result[key] = count
    for medium_source, count in ghost_pages_url_count.iteritems():
        key = "{}.nb_urls".format(medium_source)
        result[key] = count
    return result


def save_ghost_pages(source, ghost_pages, prefix, output_dir):
    """Save ghost pages as a tsv file
    :param source: the traffic source
    :type source: str
    :param ghost_pages: a list dict of tuples (nb_sessions, url)
                        that stores the ghost pages for the input source
    :type ghost_pages: list
    :param prefix: the prefix to use for the generated files
    :type prefix: str
    :param output_dir: the directory where to save the file
    :type output_dir: str
    :returns: str - the path to the generated file."""
    ghost_file_path = os.path.join(output_dir,
                                   "{}_{}.tsv".format(prefix, source))
    #save the entry in it
    with open(ghost_file_path, "w") as ghost_file:
        for nb_sessions, url in ghost_pages:
            ghost_file.write("{}\t{}\n".format(url, nb_sessions))
    return ghost_file_path


def save_ghost_pages_count(ghost_pages_count, output_dir):
    """Save the session count and the number of urls of ghost pages in a json file.
    There is one entry per traffic source.
    :param ghost_pages_session_count: a dict traffic source -> nb sessions or nb_urls
                                      keys have the form:
                                      - "organic.all.nb_visits"
                                      - "social.facebook.nb_urls"
    :type ghost_pages_session_count: dict
    :param output_dir: the directory where to save the file.
    :type output_dir: str
    :returns: str - the path to the generated file
    """
    output_file_path = os.path.join(output_dir,
                                    "ghost_pages_count.json")
    with open(output_file_path, "w") as output_file:
        count_json = json.dumps(deep_dict(ghost_pages_count))
        output_file.write(count_json)
    return output_file_path


class GoogleAnalyticsAggregator(object):
    """A page that manages aggregation of google analytics entries.
    It is able to for each considered source/medium:
    - the number of sessions
    - the number of urls
    - the pages with the most visits
    To do so, it is required to call the update() method several times.
    Each call should include the entries for a specific url.
    Without this constraint, top pages computation would be harder,
    as you would never be sure that you can drop the entries corresponding
    to an url.
    """
    def __init__(self, top_pages_nb):
        """Constructor
        :param top_pages_nb: the number of top pages to keep
        :type top_pages_nb: int
        """
        self.top_pages_nb = top_pages_nb

        self.medium_idx = RawVisitsStreamDef.field_idx("medium")
        self.source_idx = RawVisitsStreamDef.field_idx("source")
        self.social_network_idx = RawVisitsStreamDef.field_idx("social_network")
        self.sessions_idx = RawVisitsStreamDef.field_idx("nb")

        #store the pages with the most visits for each source/medium
        self.top_pages = {}
        #store the number of sessions for each source/medium
        self.session_count = Counter()
        #store the number of urls for each source/medium
        self.url_count = Counter()
        for medium, source in _iterate_sources():
            medium_source = "{}.{}".format(medium, source)
            self.top_pages[medium_source] = []
            self.session_count[medium_source] = 0
            self.url_count[medium_source] = 0

    def update(self, url_without_protocol, entries):
        """Update the aggregations with a list of entries corresponding to
        the same url.
        :param url_without_protocol: the url (without http or https)
        :type url_without_protocol: str
        :param entries: the list of google analytics entries
                        corresponding to the input url.
                        Each entry is RawVisitsStreamDef row
        :type entries: list
        """
        aggregated_session_count = self.aggregate_entries(entries)
        #update the top ghost pages for this url
        update_top_ghost_pages(self.top_pages,
                               self.top_pages_nb,
                               url_without_protocol,
                               aggregated_session_count)

        self.update_counters(aggregated_session_count,
                             self.session_count,
                             self.url_count)

    def aggregate_entries(self, entries):
        """Aggregate entries corresponding to the same url.
        Each entry is a tuple (url, medium, source, social_network, nb_sessions)
        The aggregation is made on the different search engines and social networks.
        It is also done on all organic sources and all social networks.
        :param entries: the input entries (should correspond to the same url)
        :type entries: list
        :returns: Counter - a counter that contains the number of sesssions
                            for each considered source/medium
        """
        aggregated_session_count = Counter()
        for entry in entries:
            medium = entry[self.medium_idx]
            source = entry[self.source_idx]
            social_network = entry[self.social_network_idx]
            nb_sessions = entry[self.sessions_idx]

            update_session_count(aggregated_session_count,
                                 medium,
                                 source,
                                 social_network,
                                 nb_sessions)

        return aggregated_session_count

    def update_counters(self,
                        aggregated_session_count,
                        session_count,
                        url_count):
        """Update the session count and the url count
        :param aggregated_session_count: a Counter representing the visits
                                         for a given url for each source/medium
        :type aggregated_session_count: Counter
        :param pages_session_count: a Counter that stores the number of sessions
                                    for each considered source/medium
        :type pages_url_count: a Counter that stores the number of urls
                               for each considered source/medium
        """
        #update the session count
        session_count.update(aggregated_session_count)

        #the number of urls for each medium/source is at most 1
        #since we are processing all entries of the same url
        url_count.update(
            Counter(aggregated_session_count.keys())
        )
