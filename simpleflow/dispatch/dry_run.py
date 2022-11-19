def do_nothing(*args, **kwargs):
    return {}


class DryRunDispatcher:
    def dispatch(self, name):
        return do_nothing
