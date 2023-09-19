from __future__ import annotations

from simpleflow.swf.mapper.core import ConnectedSWFObject


class BaseQuerySet(ConnectedSWFObject):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get(self, *args, **kwargs):
        raise NotImplementedError

    def get_or_create(self, *args, **kwargs):
        raise NotImplementedError

    def filter(self, *args, **kwargs):
        raise NotImplementedError

    def all(self, *args, **kwargs):
        raise NotImplementedError

    def create(self, *args, **kwargs):
        raise NotImplementedError
