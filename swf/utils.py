# -*- coding: utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from datetime import datetime, timedelta
from time import mktime
from itertools import chain, izip, islice

from functools import wraps

# De-capitalize a string (lower first character)
decapitalize = lambda s: s[:1].lower() + s[1:] if s else ''

# Get a past day datetime
past_day = lambda d: datetime.now() - timedelta(days=d)

# Get a datetime timestamp
datetime_timestamp = lambda datetime: mktime(datetime.timetuple())


class Enum(object):
    """Enumeration type

    Examples
    --------

    >>> Numbers = Enum('ZERO', 'ONE', 'TWO')
    >>> Numbers.ZERO
    0
    >>> Numbers.ONE
    1

    or

    >>> Numbers = Enum(zero="ZERO", one="ONE")
    >>> Numbers.zero
    'ZERO'

    Inspired from:
    http://stackoverflow.com/questions/36932/whats-the-best-way-to-implement-an-enum-in-python

    """
    def __init__(self, *sequential, **named):
        self.enums = dict(zip(sequential, range(len(sequential))), **named)

        for key, value in self.enums.viewitems():
            setattr(self, key, value)

    def __contains__(self, key):
        return key in self.enums

    def has_member(self, member):
        return any(value == member for value in self.enums.itervalues())


def get_subkey(d, key_path):
    """Gets a sub-dict key, and return None if whether
    the parent or child dict key does not exist

    :param  d: dict to operate over
    :type   d: dict of dicts

    :param  key_path: dict keys path list representation
    :type   key_path: list

    Example
    -------

    >>> d = {
    ...   'a': {
    ...     '1': 2,
    ...     '2': 3,
    ...   }
    ... }
    >>> get_subkey(d, ['a'])
    {'1': 2, '2': 3}
    >>> get_subkey(d, ['a', '1'])
    2
    >>> get_subkey(d, ['a', '3'])

    """
    if len(key_path) > 1:
        if d.get(key_path[0]) is None:
            return None
        return get_subkey(d[key_path[0]], key_path[1:])
    else:
        return d.get(key_path[0])


class _CachedProperty(property):
    """A property cache mechanism.

    The cache is stored on the model as a protected attribute. Expensive
    property lookups, such as database access, can therefore be sped up
    when accessed multiple times in the same request.

    The property can also be safely set and deleted without interference.
    """

    def __init__(self, fget, fset=None, fdel=None, doc=None):
        """Initializes the cached property."""
        self._cache_name = "_{name}_cache".format(
            name=fget.__name__,
        )
        # Wrap the accessors.
        fget = self._wrap_fget(fget)
        if callable(fset):
            fset = self._wrap_fset(fset)
        if callable(fdel):
            fdel = self._wrap_fdel(fdel)
        # Create the property.
        super(_CachedProperty, self).__init__(fget, fset, fdel, doc)

    def _wrap_fget(self, fget):
        @wraps(fget)
        def do_fget(obj):
            if hasattr(obj, self._cache_name):
                return getattr(obj, self._cache_name)
            # Generate the value to cache.
            value = fget(obj)
            setattr(obj, self._cache_name, value)
            return value
        return do_fget

    def _wrap_fset(self, fset):
        @wraps(fset)
        def do_fset(obj, value):
            fset(obj, value)
            setattr(obj, self._cache_name, value)
        return do_fset

    def _wrap_fdel(self, fdel):
        @wraps(fdel)
        def do_fdel(obj):
            fdel(obj)
            delattr(obj, self._cache_name)
        return do_fdel
cached_property = _CachedProperty


def immutable(mutableclass):
    """ Decorator for making a slot-based class immutable

    Source: http://code.activestate.com/recipes/578233-immutable-class-decorator/
    """

    if not isinstance(type(mutableclass), type):
        raise TypeError('@immutable: must be applied to a new-style class')
    if not hasattr(mutableclass, '__slots__'):
        raise TypeError('@immutable: class must have __slots__')

    class immutableclass(mutableclass):
        __slots__ = ()                      # No __dict__, please

        def __new__(cls, *args, **kw):
            new = mutableclass(*args, **kw) # __init__ gets called while still mutable
            new.__class__ = immutableclass  # locked for writing now
            return new

        def __init__(self, *args, **kw):    # Prevent re-init after __new__
            pass

    # Copy class identity:
    immutableclass.__name__ = mutableclass.__name__
    immutableclass.__module__ = mutableclass.__module__

    # Make read-only:
    for name, member in mutableclass.__dict__.items():
        if hasattr(member, '__set__'):
            setattr(immutableclass, name, property(member.__get__))

    return immutableclass


def camel_to_underscore(string):
    """Translates amazon Camelcased strings to
    lowercased underscored strings"""
    res = []

    for index, char in enumerate(string):
        if index != 0 and char.isupper():
            res.extend(['_', char.lower()])
        else:
            res.extend([char.lower()])

    return ''.join(res)


def underscore_to_camel(string):
    """

    >>> underscore_to_camel('')
    ''
    >>> underscore_to_camel('a')
    'A'
    >>> underscore_to_camel('A')
    'A'
    >>> underscore_to_camel('ab')
    'Ab'
    >>> underscore_to_camel('a_b_c')
    'ABC'
    >>> underscore_to_camel('ab_cd_ef')
    'AbCdEf'
    >>> underscore_to_camel('request_cancel_workflow')
    'RequestCancelWorkflow'

    """
    if string == '':
        return ''

    return ''.join(chain([string[0].upper()],
                         ((c.upper() if p == '_' else c) if
                          c != '_' else '' for p, c in
                          izip(islice(string, 0, None),
                               islice(string, 1, None)))))
