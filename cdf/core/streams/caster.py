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


def return_value(value, cast_func, options):
    if value == 'missing' and 'missing' in options:
        return cast_func(options['missing'])
    elif value == '' and 'default' in options:
        return cast_func(options['default'])
    return cast_func(value)


class Caster(object):
    """
    Cast each field value to an object with respect to a definition mapping in
    *fields*.

    """
    def __init__(self, fields):
        self._fields = []
        for field in fields:
            # Add empty options if the field definition has only 2 values (field_name, caster_func)
            if len(field) == 2:
                self._fields.append(field + ({},))
            else:
                self._fields.append(field)

    def cast_line(self, line):
        return [(return_value(value, cast, options) if cast else value) for
                (name, cast, options), value in izip_longest(self._fields, line, fillvalue='missing')]

    def cast(self, iterable):
        for i in iterable:
            yield self.cast_line(i)
