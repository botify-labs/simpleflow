class S3KeyNotFound(Exception):
    def __init__(self, key):
        self.key
