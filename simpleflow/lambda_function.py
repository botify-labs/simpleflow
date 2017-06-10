from simpleflow.base import Submittable


class LambdaFunction(Submittable):
    def __init__(self,
                 name,
                 start_to_close_timeout=None,
                 idempotent=None,
                 is_python_function=True,
                 ):
        self.name = name
        self.start_to_close_timeout = start_to_close_timeout
        self.idempotent = idempotent
        self.is_python_function = is_python_function
