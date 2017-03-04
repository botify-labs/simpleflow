from simpleflow.base import Submittable


class LambdaFunction(Submittable):
    def __init__(self,
                 name,
                 start_to_close_timeout=None,
                 idempotent=None):
        self.name = name
        self.start_to_close_timeout = start_to_close_timeout
        self.idempotent = idempotent
