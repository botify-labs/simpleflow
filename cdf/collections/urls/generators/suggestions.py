from BQL.parser.tagging import query_to_python


def transform_queries(queries_lst):
    return [
        {'func': query_to_python(query),
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
