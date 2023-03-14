# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

from swf.core import ConnectedSWFObject


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
