import os

from cdf.features.ganalytics.settings import ORGANIC_SOURCES, SOCIAL_SOURCES
from cdf.features.ganalytics.streams import RawVisitsStreamDef


def update_ghost_pages(ghost_pages, entry):
    """Update the dict that stores the ghost pages.
    :param ghost_pages: a dict source -> ghost_pages where ghost_pages is
                        a dict url -> nb sessions
                        that stores the ghost pages.
                        It will be updated by the function
    :type ghost_pages: dict
    :param entry: the entry to use to update the ghost pages.
                  this is a RawVisitsStreamDef entry
    :type entry: list
    """
    url = entry[RawVisitsStreamDef.field_idx("url")]
    nb_sessions = entry[RawVisitsStreamDef.field_idx("nb")]
    for source in get_sources(entry):
        if source not in ghost_pages:
            ghost_pages[source] = {}
        if url not in ghost_pages[source]:
            ghost_pages[source][url] = 0
        ghost_pages[source][url] += nb_sessions


def get_sources(entry):
    """Returns a list of traffic sources the input entry contributes to.
    For instance a visit from google counts as an organic visit but also
    as a google visit.
    Thus this function will return ["organic", "visit"]
    :param entry: the input stream entry to consider
                  This is an entry from a RawVisitsStreamDef
    :type entry: list
    :returns: list
    """
    medium = entry[RawVisitsStreamDef.field_idx("medium")]
    source = entry[RawVisitsStreamDef.field_idx("source")]
    social_network = entry[RawVisitsStreamDef.field_idx("social_network")]
    result = []
    if medium == "organic":
        result.append("organic")
        if source in ORGANIC_SOURCES:
            result.append(source)

    if social_network is not None:
        result.append("social")
        if social_network in SOCIAL_SOURCES:
            result.append(social_network)
    return result


def save_ghost_pages(source, ghost_pages, output_dir):
    """Save ghost pages as a tsv file
    :param source: the traffic source
    :type source: str
    :param ghost_pages: a dict url -> nb_sessions that stores the ghost pages
                        for the input source
    :type ghost_pages: dict
    :param output_dir: the directory where to save the file
    :type output_dir: str
    :returns: str - the path to the generated file."""
    #TODO: save it as gzip?
    ghost_file_path = os.path.join(output_dir,
                                   "top_ghost_pages_{}.tsv".format(source))
    #save the entry in it
    with open(ghost_file_path, "w") as ghost_file:
        for url, nb_sessions in ghost_pages.iteritems():
            ghost_file.write("{}\t{}\n".format(url, nb_sessions))
    return ghost_file_path
