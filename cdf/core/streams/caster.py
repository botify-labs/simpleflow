from itertools import izip_longest

"""
A stream is a generator of values. A value may be any object but usually is a
string or a tuple. The purpose of this module is to provide simple operations
that are easy to compose.

A stream allow to chain operations on each of its elements and consume the
values on-demand. Calling a stream operation after another does not iterate two
times on the values.

You can pass options when defining a field

* default : if column is empty, return the default value
* missing : if column is missing, return the missing value

Example:

>>> import streams.utils
>>> INFOS_FIELDS = [('id', int),
...                 ('depth', int),
...                 ('date_crawled', None),
...                 ('http_code', int, {'default': 200}),
...                 ('byte_size', int),
...                 ('delay1', int),
...                 ('delay2', bool),
...                 ('gzipped', bool, {'missing': True})]
>>> cast = Caster(INFOS_FIELDS).cast
>>> inlinks = cast(streams.utils.split_file((open('test.data'))))

"""

__all__ = ['Caster']

MISSING_OPTION = 'missing'
DEFAULT_OPTION = 'default'

MISSING_VALUE = '[missing]'


class FieldCaster(object):
    def cast(value):
        pass


class BasicFieldCaster(FieldCaster):
    def __init__(self, cast):
        self._cast = cast

    def cast(self, value):
        return self._cast(value)


class AdvancedFieldCaster(FieldCaster):
    def __init__(self, cast, options):
        self._cast = cast
        self.options = options
        self.missing_value = None
        if MISSING_OPTION in self.options:
            self.missing_value = self._cast(self.options[MISSING_OPTION])
        elif DEFAULT_OPTION in self.options:
            self.missing_value = self._cast(self.options[DEFAULT_OPTION])
        else:
            self.missing_value = self._cast('')

    def cast(self, value):
        if value == MISSING_VALUE:
            return self.missing_value
        elif value == '' and DEFAULT_OPTION in self.options:
            return self._cast(self.options[DEFAULT_OPTION])
        return self._cast(value)


class Caster(object):
    """
    Cast each field value to an object with respect to a definition mapping in
    *fields*.

    """
    def __init__(self, fields):
        self.no_missing_field = all([len(field) == 2 for field in fields])
        self.casters = []
        for field in fields:
            # Add empty options if the field definition has only 2 values (field_name, caster_func)
            if len(field) == 2:
                name, cast = field
                self.casters.append(cast)
            else:
                name, cast, options = field
                caster = AdvancedFieldCaster(cast, options)
                self.casters.append(caster.cast)

    def cast(self, iterable):
        #simple case, we can simply apply the cast functions
        for line in iterable:
            yield [cast(value) for cast, value in izip_longest(self.casters, line, fillvalue=MISSING_VALUE)]


