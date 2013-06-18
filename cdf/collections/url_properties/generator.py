from cdf.collection.url_properties.validator import ResourceTypeValidator


class UrlPropertiesGenerator(object):

    def __init__(self, stream_patterns, resource_type_settings):
        self.stream_patterns = stream_patterns
        self.resource_type_settings = resource_type_settings
        self.resource_type_validator = ResourceTypeValidator(resource_type_settings)

    def is_valid(self):
        return self.resource_type_validator.is_valid()

    """
    Return a dict with the following format : 
    {
        "resource_type": "value"
    }
    """
    def __iter__(self):
        for i in self.streams_patterns:
            url_id, protocol, host, path, query_string = i
            url = ''.join((protocol, host, path, query_string))
