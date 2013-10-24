# -*- coding: utf-8 -*-
from cdf.streams.mapping import CONTENT_TYPE_INDEX, CONTENT_TYPE_NAME_TO_ID
from cdf.streams.utils import idx_from_stream, group_left
from BQL.parser.tagging import query_to_python
from BQL.parser.metadata import query_to_python as metadata_query_to_python
from cdf.utils.hashing import string_to_int32
from cdf.collections.urls.constants import CLUSTER_TYPE_TO_ID

import numpy
from pandas import Series


def transform_queries(queries_lst, func=query_to_python):
    transformed = []
    for query in queries_lst:
        if query == "Unrecognized pattern":
            continue
        try:
            _func = func(query)
        except Exception, e:
            raise Exception('Query cannot be parsed : {} / Message: {}'.format(query, e))

        transformed.append(
            {'func': _func,
             'hash': string_to_int32(query),
             'string': query}
        )
    return transformed


class MetadataClusterMixin(object):

    def __init__(self):
        self.patterns_clusters = dict()
        self.metadata_clusters = dict()

    def add_pattern_cluster(self, pattern_name, cluster_list):
        self.patterns_clusters[pattern_name] = transform_queries(cluster_list, query_to_python)

    def add_metadata_cluster(self, metadata_type, cluster_list):
        if metadata_type not in CONTENT_TYPE_INDEX.values():
            raise Exception('{} is not a valid metadata type'.format(metadata_type))
        self.metadata_clusters[CONTENT_TYPE_NAME_TO_ID[metadata_type]] = transform_queries(cluster_list, metadata_query_to_python)

    def make_clusters_series(self):
        """
        Generate a pandas Series object with index as hash and value as the full request
        """
        final_serie = None
        for cluster_section, clusters in (("pattern", self.patterns_clusters), ("metadata", self.metadata_clusters)):
            for i, (cluster_name, suggestions) in enumerate(clusters.iteritems()):
                cluster_id = CLUSTER_TYPE_TO_ID[cluster_section][cluster_name]
                if len(suggestions) == 0:
                    continue
                # Temporary deduplicate queries, some are set 2 times like in path file for francetvinfo, Simon is fixing it
                suggestions = list(set((q['hash'], q['string']) for q in suggestions))
                serie = Series([q[1] for q in suggestions], index=[int(str(cluster_id) + str(q[0])) for q in suggestions], dtype=numpy.character)
                if final_serie is None:
                    final_serie = serie.copy()
                else:
                    final_serie = final_serie.append(serie)
        return final_serie


class UrlSuggestionsGenerator(MetadataClusterMixin):

    def __init__(self, stream_patterns, stream_infos, stream_contents):
        super(UrlSuggestionsGenerator, self).__init__()
        self.stream_patterns = stream_patterns
        self.stream_infos = stream_infos
        self.stream_contents = stream_contents

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
                cluster_id = CLUSTER_TYPE_TO_ID["pattern"][cluster]
                for query in queries:
                    if query['func'](url, protocol, host, path, query_string, locator):
                        yield (url_id, str(cluster_id) + str(query["hash"]))

            for entry in streams['contents']:
                url_id, metadata_type, hash_id, value = entry
                if metadata_type not in self.metadata_clusters:
                    continue
                for query in self.metadata_clusters[metadata_type]:
                    cluster_id = CLUSTER_TYPE_TO_ID["metadata"][metadata_type]
                    if query['func'](value):
                        yield (url_id, str(cluster_id) + str(query["hash"]))

    def save_to_file(self, location):
        f = open(location, 'w')
        for data in self:
            f.write('\t'.join(data) + '\n')
        f.close()
