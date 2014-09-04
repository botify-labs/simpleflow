# -*- coding: utf-8 -*-
from pandas import DataFrame

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
