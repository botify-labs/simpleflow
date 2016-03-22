# -*- coding:utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from collections import namedtuple, OrderedDict

from swf.core import ConnectedSWFObject
from swf.exceptions import DoesNotExistError


Difference = namedtuple('Difference', ('attr', 'local', 'upstream'))


class ModelDiff(object):
    """Holds differences between local and upstream model version.

    :param  input: triples (tuples) storing in order: compared attribute name,
                   local model attribute value, upstream model attribute value.
    :type   input: *args
    """
    def __init__(self, *input):
        self.container = self._process_input(input)

    def __contains__(self, attr):
        return attr in self.container

    def __len__(self):
        return len(self.container)

    def __getitem__(self, index):
        attr, (local, upstream) = self.container.items()[index]
        return Difference(attr, local, upstream)

    def _process_input(self, input):
        return OrderedDict((attr, (local, upstream)) for
                           attr, local, upstream in input if
                           local != upstream)

    def add_input(self, *input):
        """Adds input differing data into ModelDiff instance"""
        self.container.update(self._process_input(input))

    def merge(self, model_diff):
        """Merges another ModelDiff instance into the current one"""
        self.container.update(model_diff.container)

    def differing_fields(self):
        """Returns the name of fields differing from upstream"""
        return self.container.keys()

    def as_list(self):
        """Outputs models differences as a list of
        swf.models.base.Difference namedtuple
        """
        return [
            Difference(k, v[0], v[1]) for k, v
            in self.container.iteritems()
        ]


class BaseModel(ConnectedSWFObject):
    __slots__ = [
        'exists',
        'is_synced',
        'changes',
    ]

    def _diff(self):
        """Checks for differences between current model instance
        and upstream version"""
        raise NotImplementedError

    @property
    def exists(self):
        """Checks if the connected swf object exists amazon-side"""
        raise NotImplementedError

    @property
    def is_synced(self):
        """Checks if current Model instance has changes, comparing
        with remote object representation

        :rtype: bool
        """
        try:
            return bool(self._diff == [])
        except DoesNotExistError:
            return False

    @property
    def changes(self):
        """Returns changes between current model instance, and
        remote object representation

        :returns: A list of swf.models.base.ModelDiff namedtuple describing
                  differences
        :rtype: list
        """
        return self._diff()

    def save(self):
        """Creates the connected swf object amazon side"""
        raise NotImplementedError

    def delete(self):
        """Deprecates the connected swf object amazon side"""
        raise NotImplementedError

    def upstream(self):
        """Instantiates a new upstream version of the model"""
        raise NotImplementedError
