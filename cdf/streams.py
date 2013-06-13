class BaseStream(object):

    def __init__(self, serializer):
        self.serializer = serializer

    def __iter__(self):
        for k in self.local_iter():
            if self.serializer:
                yield self.serializer.loads(k)
            else:
                yield k

    def next(self):
        if self.serializer:
            return self.serializer.loads(self.local_next())
        return self.local_next()


class FileStream(BaseStream):

    def __init__(self, location, serializer=None):
        super(FileStream, self).__init__(serializer)
        self.f = open(location)

    def local_iter(self):
        for l in self.f:
            yield l[:-1]

    def local_next(self):
        return self.f.next()

    def seek(self, position):
        self.f.seek(position)


class ListStream(BaseStream):

    def __init__(self, lst, serializer=None):
        self.lst = lst
        self.i = -1
        super(ListStream, self).__init__(serializer)

    def local_iter(self):
        for k in self.lst:
            yield k

    def local_next(self):
        self.i += 1
        if self.i < len(self.lst):
            return self.lst[self.i]
        else:
            raise StopIteration

    def seek(self, position):
        self.i = position - 1
