# -*- coding: utf-8 -*-
from cdf.streams.mapping import CONTENT_TYPE_INDEX, CONTENT_TYPE_NAME_TO_ID
from BQL.parser.tagging import query_to_python
from BQL.parser.metadata import query_to_python as metadata_query_to_python


def transform_queries(queries_lst, func=query_to_python):
    return [
        {'func': func(query),
         'string': query}
        for query in queries_lst
    ]


class UrlSuggestionsGenerator(object):

    def __init__(self, stream_patterns, clusters_dict):
        self.stream_patterns = stream_patterns
        self.clusters_dict = {name: transform_queries(queries) for name, queries in clusters_dict.iteritems()}

    def __iter__(self):
        for entry in self.stream_patterns:
            url_id, protocol, host, path, query_string = entry
            # locator not yet in urlids.txt
            locator = ''
            url = "{}://{}{}{}".format(protocol, host, path, query_string)

            for cluster, queries in self.clusters_dict.iteritems():
                for query in queries:
                    if query['func'](url, protocol, host, path, query_string, locator):
                        yield (url_id, cluster, query['string'])


class MetadataSuggestionsGenerator(object):
    def __init__(self, stream_contents):
        self.stream_contents = stream_contents
        self.clusters_dict = dict()

    def add_cluster(self, metadata_type, cluster_list):
        if metadata_type not in CONTENT_TYPE_INDEX.values():
            raise Exception('{} is not a valid metadata type'.format(metadata_type))
        self.clusters_dict[CONTENT_TYPE_NAME_TO_ID[metadata_type]] = transform_queries(cluster_list, metadata_query_to_python)

    def __iter__(self):
        for entry in self.stream_contents:
            url_id, metadata_type, hash_id, value = entry
            for query in self.clusters_dict[metadata_type]:
                if query['func'](value):
                    yield (url_id, "metadata_{}".format(CONTENT_TYPE_INDEX[metadata_type]), query["string"])
