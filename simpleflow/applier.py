class Applier(object):
    def __init__(self, method, *args, **kwargs):
        self.method = method
        self.args = args
        self.kwargs = kwargs

    def call(self):
        if hasattr(self.method, 'execute'):
            return self.method(*self.args, **self.kwargs).execute()
        else:
            return self.method(*self.args, **self.kwargs)
