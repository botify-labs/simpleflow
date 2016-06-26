# -*- coding: utf-8 -*-


class MiniMock(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
