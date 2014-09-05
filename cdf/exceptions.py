class MissingResource(Exception):
    pass


class InvalidDataFormat(Exception):
    pass


class MalformedFileNameError(Exception):
    pass


class ElasticSearchIncompleteIndex(Exception):
    pass


class BotifyQueryException(Exception):
    pass


class ConfigurationError(Exception):
    pass


class ApiError(Exception):
    pass


#raised when the format returned by the API is wrong (or unexpected)
class ApiFormatError(Exception):
    pass
