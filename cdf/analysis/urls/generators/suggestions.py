# -*- coding: utf-8 -*-
from pandas import DataFrame

from cdf.core.streams.utils import group_left
from cdf.analysis.urls.constants import CLUSTER_TYPE_TO_ID
from cdf.features.main.streams import InfosStreamDef
from cdf.features.semantic_metadata.settings import CONTENT_TYPE_INDEX, CONTENT_TYPE_NAME_TO_ID


def transform_queries(queries_lst):
    return [{'hash': hash, 'string': query, 'verbose_string': verbose_string}
            for query, verbose_string, hash in queries_lst]


class MetadataClusterMixin(object):

    def __init__(self):
        self.patterns_clusters = dict()
        self.metadata_clusters = dict()

    def add_pattern_cluster(self, pattern_name, cluster_list):
        self.patterns_clusters[pattern_name] = transform_queries(cluster_list)

    def add_metadata_cluster(self, metadata_type, cluster_list):
        if metadata_type not in CONTENT_TYPE_INDEX.values():
            raise Exception('{} is not a valid metadata type'.format(metadata_type))
        self.metadata_clusters[CONTENT_TYPE_NAME_TO_ID[metadata_type]] = transform_queries(cluster_list)

    def make_clusters_dataframe(self):
        """
        Generate a pandas DataFrame object with hash as index
        and bql query and verbose query as values
        """

        final_dataframe = DataFrame()
        for cluster_section, clusters in (("pattern", self.patterns_clusters), ("metadata", self.metadata_clusters)):
            for i, (cluster_name, suggestions) in enumerate(clusters.iteritems()):
                if len(suggestions) == 0:
                    continue
                # Temporary deduplicate queries, some are set 2 times like in path file for francetvinfo, Simon is fixing it
                suggestions = list(set((q['hash'], q['string'], q['verbose_string']) for q in suggestions))
                dataframe = DataFrame({"string": [q[1] for q in suggestions],
                                       "verbose_string": [q[2] for q in suggestions]})
                dataframe.index = [q[0] for q in suggestions]
                final_dataframe = final_dataframe.append(dataframe)

        return final_dataframe


class UrlSuggestionsGenerator(MetadataClusterMixin):

    def __init__(self, stream_patterns, stream_infos, stream_contents):
        super(UrlSuggestionsGenerator, self).__init__()
        self.stream_patterns = stream_patterns
        self.stream_infos = stream_infos
        self.stream_contents = stream_contents

    def __iter__(self):
        http_code_idx = InfosStreamDef.field_idx('http_code')
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
