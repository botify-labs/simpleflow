# -*- coding: utf-8 -*-
from cdf.streams.mapping import CONTENT_TYPE_INDEX, CONTENT_TYPE_NAME_TO_ID
from cdf.streams.utils import idx_from_stream, group_left
from BQL.parser.tagging import query_to_python
from BQL.parser.metadata import query_to_python as metadata_query_to_python


def transform_queries(queries_lst, func=query_to_python):
    return [
        {'func': func(query),
         'string': query}
        for query in queries_lst
    ]


class UrlSuggestionsGenerator(object):

    def __init__(self, stream_patterns, stream_infos, stream_contents):
        self.stream_patterns = stream_patterns
        self.stream_infos = stream_infos
        self.stream_contents = stream_contents
        self.patterns_clusters = dict()
        self.metadata_clusters = dict()

    def add_pattern_cluster(self, pattern_name, cluster_list):
        self.patterns_clusters[pattern_name] = transform_queries(cluster_list, query_to_python)

    def add_metadata_cluster(self, metadata_type, cluster_list):
        if metadata_type not in CONTENT_TYPE_INDEX.values():
            raise Exception('{} is not a valid metadata type'.format(metadata_type))
        self.metadata_clusters[CONTENT_TYPE_NAME_TO_ID[metadata_type]] = transform_queries(cluster_list, metadata_query_to_python)

    def __iter__(self):
        http_code_idx = idx_from_stream('infos', 'http_code')
        for i in group_left((self.stream_patterns, 0), infos=(self.stream_infos, 0), contents=(self.stream_contents, 0)):

            url_id, left_line, streams = i
            # If http_code in 0, 1, 2 > means than not crawled
            # If http_code < 0, it means that there were a fetcher error but the url was correctly crawled
            if streams['infos'][0][http_code_idx] in (0, 1, 2):
                continue

            url_id, protocol, host, path, query_string = left_line
            # locator not yet in urlids.txt
            locator = ''
            url = "{}://{}{}{}".format(protocol, host, path, query_string)

            for cluster, queries in self.patterns_clusters.iteritems():
                for query in queries:
                    if query['func'](url, protocol, host, path, query_string, locator):
                        yield (url_id, cluster, query['string'])

            for entry in streams['contents']:
                url_id, metadata_type, hash_id, value = entry
                for query in self.metadata_clusters[metadata_type]:
                    if query['func'](value):
                        yield (url_id, "metadata_{}".format(CONTENT_TYPE_INDEX[metadata_type]), query["string"])

    def save_to_file(self, location):
        f = open(location, 'w')
        for data in self:
            f.write('\t'.join(data) + '\n')
        f.close()
