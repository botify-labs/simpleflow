from cdf.collections.urls.tagging.resource_type import compile_resource_type_settings
from cdf.streams.utils import idx_from_stream, group_left


class UrlTaggingGenerator(object):

    def __init__(self, stream_patterns, stream_infos, resource_type_settings):
        self.stream_patterns = stream_patterns
        self.stream_infos = stream_infos
        self.resource_type_settings = resource_type_settings
        self.resource_type_compiled = compile_resource_type_settings(self.resource_type_settings)

    def __iter__(self):
        """
        Return a tuple where first value is the url_id and second value is a dict
        Ex :
        (1, {
            "resource_type": "value"
        }
        """
        date_crawled_idx = idx_from_stream('infos', 'date_crawled')
        for i in group_left((self.stream_patterns, 0), infos=(self.stream_infos, 0)):
            url_id, left_line, streams = i
            date_crawled = streams['infos'][0][date_crawled_idx]
            if not date_crawled:
                continue

            url_id, protocol, host, path, query_string = left_line
            # locator not yet in urlids.txt
            locator = ''
            url = "{}://{}{}{}".format(protocol, host, path, query_string)
            parents_matches = set()
            found = False

            for rule in self.resource_type_compiled:
                """
                If the rule inherits from another one but the url did not match
                with the parent, we pass
                """
                if 'inherits_from' in rule and rule['inherits_from'] not in parents_matches:
                    continue

                if rule['query'](url, protocol, host, path, query_string, locator):
                    if not rule.get('abstract', False):
                        found = True
                        yield (url_id, {"resource_type": rule['value'],
                                        "host": host})
                        break
                    else:
                        parents_matches.add(rule['rule_id'])
            if not found:
                yield (url_id, {"resource_type": "unknown",
                                "host": host})

    def save_to_file(self, location):
        f = open(location, 'w')
        for url_id, document in self:
            f.write('%d\t%s\n' % (url_id, document['resource_type']))
        f.close()
