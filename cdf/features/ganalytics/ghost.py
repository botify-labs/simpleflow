import os
import json
import heapq

from cdf.utils.dict import deep_dict
from cdf.features.ganalytics.settings import ORGANIC_SOURCES, SOCIAL_SOURCES


def update_session_count(ghost_pages, medium, source, social_network, nb_sessions):
    """Update the dict that stores the ghost pages.
    :param ghost_pages: a dict medium/source -> ghost_pages where ghost_pages is
                        a dict url -> nb sessions
                        that stores the ghost pages.
                        It will be updated by the function.
                        Keys have the form "organic.all", "organic.google", etc.
    :type ghost_pages: dict
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
    for medium_source in get_sources(medium, source, social_network):
        if medium_source not in ghost_pages:
            ghost_pages[medium_source] = 0
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
    :param session_count: a list of tuples (nb_sessions, source)
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


def update_ghost_pages_session_count(ghost_pages_session_count,
                                     session_count):
    """Update the number of sessions for ghost pages for each considered
    medium/source combination.
    :param ghost_pages_sessions_count: a dict medium/source -> nb sessions
                                       that stores the number of sessions
                                       for each considerd medium/source
                                       medium/source have the form
                                       "organic.all", "social.facebook", etc
    :type ghost_pages_session_count: dict
    :param session_count: the session data corresponding to a ghost url.
                          It is dict medium/source -> nb sessions
    :type session_count: dict
    """
    for medium_source, count in session_count.iteritems():
        if medium_source not in ghost_pages_session_count:
            ghost_pages_session_count[medium_source] = 0
        ghost_pages_session_count[medium_source] += count


def get_sources(medium, source, social_network):
    """Returns a list of traffic sources the input entry contributes to.
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


def save_ghost_pages(source, ghost_pages, output_dir):
    """Save ghost pages as a tsv file
    :param source: the traffic source
    :type source: str
    :param ghost_pages: a list dict of tuples (nb_sessions, url)
                        that stores the ghost pages for the input source
    :type ghost_pages: list
    :param output_dir: the directory where to save the file
    :type output_dir: str
    :returns: str - the path to the generated file."""
    ghost_file_path = os.path.join(output_dir,
                                   "top_ghost_pages_{}.tsv".format(source))
    #save the entry in it
    with open(ghost_file_path, "w") as ghost_file:
        for nb_sessions, url in ghost_pages:
            ghost_file.write("{}\t{}\n".format(url, nb_sessions))
    return ghost_file_path


def save_ghost_pages_session_count(ghost_pages_session_count, output_dir):
    """Save the session count of ghost pages in a json file.
    There is one entry per traffic source.
    :param ghost_pages_session_count: a dict traffic source -> nb sessions
    :type ghost_pages_session_count: dict
    :param output_dir: the directory where to save the file.
    :type output_dir: str
    :returns: str - the path to the generated file
    """
    output_file_path = os.path.join(output_dir,
                                    "ghost_pages_session_count.json")
    with open(output_file_path, "w") as output_file:
        session_count_json = json.dumps(deep_dict(ghost_pages_session_count))
        output_file.write(session_count_json)
    return output_file_path
