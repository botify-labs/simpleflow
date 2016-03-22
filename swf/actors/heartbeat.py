from .helpers import Every, meanwhile


class heartbeat_on(object):
    def __init__(self, heartbeat, interval):
        self._heartbeat = heartbeat
        self._interval = interval

    def __call__(self, func):
        from functools import wraps

        @wraps(func)
        def with_heartbeat(*args, **kwargs):
            return meanwhile(Every(self._interval,
                                   self._heartbeat, *args, **kwargs),
                             func, *args, **kwargs)

        return with_heartbeat
