from . import futures


def get_actual_value(value):
    """
    Unwrap the result of a Future or return the value.
    """
    if isinstance(value, futures.Future):
        return futures.get_result_or_raise(value)
    return value


class Applier(object):
    def __init__(self, method, *args, **kwargs):
        self.method = method
        self.args = [get_actual_value(arg) for arg in args]
        self.kwargs = {key: get_actual_value(val) for
                       key, val in kwargs.iteritems()}

    def call(self):
        if hasattr(self.method, 'execute'):
            return self.method(*self.args, **self.kwargs).execute()
        else:
            return self.method(*self.args, **self.kwargs)
