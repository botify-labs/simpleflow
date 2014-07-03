class UrlKeyDecodingError(Exception):
    """Error when decoding the composed key
    """
    def __init__(self, url_key):
        msg = 'KV Store url key decoding error: {}'.format(url_key)
        super(UrlKeyDecodingError, self).__init__(msg)
