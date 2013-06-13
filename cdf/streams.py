class BaseStream(object):
    pass


class FileStream(BaseStream):

    def __init__(self, location, serializer=None):
        self.f = open(location)
        self.serializer = serializer

    def __iter__(self):
        for k in self.f:
            if self.serializer:
                yield self.serializer.loads(k[:-1])
            else:
                yield k[:-1]

    def next(self):
        if self.serializer:
            return self.serializer.loads(self.f.next())


class ListStream(BaseStream):

    def __init__(self, lst):
        self.lst = lst
        self.i = -1

    def iter(self):
        for k in self.lst:
            yield k

    def next(self):
        self.i += 1
        if self.i < len(self.lst):
            return self.lst[self.i]
        else:
            raise StopIteration
