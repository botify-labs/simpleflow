def do_nothing(*args, **kwargs):
    return {}


class DryRunDispatcher(object):
    def dispatch(self, name):
        return do_nothing
