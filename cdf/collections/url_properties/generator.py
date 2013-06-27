from cdf.collections.url_properties.resource_type import compile_resource_type_settings


class UrlPropertiesGenerator(object):

    def __init__(self, stream_patterns, resource_type_settings):
        self.stream_patterns = stream_patterns
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
        for i in self.stream_patterns:
            url_id, protocol, host, path, query_string = i
            # locator not yet in urlids.txt
            locator = ''

            parents_matches = set()
            found = False

            for rule in self.resource_type_compiled:
                """
                If the rule inherits from another one but the url did not match
                with the parent, we pass
                """
                if 'inherits_from' in rule and rule['inherits_from'] not in parents_matches:
                    continue

                if rule['query'](protocol, host, path, query_string, locator):
                    if not rule.get('abstract', False):
                        found = True
                        yield (url_id, {"resource_type": rule['value'], "_parent": url_id, "id": url_id})
                        break
                    else:
                        parents_matches.add(rule['rule_id'])
            if not found:
                yield (url_id, {"resource_type": "unkown", "_parent": url_id, "id": url_id})

    def save_to_file(self, location):
        f = open(location, 'w')
        for url_id, document in self:
            f.write('%d\t%s\n' % (url_id, document['resource_type']))
        f.close()
