class UrlKeyDecodingError(Exception):
    """Error when decoding the composed key
    """
    def __init__(self, url_key):
        msg = 'KV Store url key decoding error: {}'.format(url_key)
        super(UrlKeyDecodingError, self).__init__(msg)


class UrlIdFieldFormatError(Exception):
    """Url id field has an unknown format
    """
    def __init__(self, field_path, field_value):
        msg = 'Url_id field: {}\'s format is not valid: {}'
        msg = msg.format(field_path, field_value)
        super(UrlIdFieldFormatError, self).__init__(msg)