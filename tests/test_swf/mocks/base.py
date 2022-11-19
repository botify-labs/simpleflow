class MiniMock:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
