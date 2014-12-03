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

import abc
from itertools import imap


__all__ = ['Caster']

MISSING_OPTION = 'missing'
DEFAULT_OPTION = 'default'

MISSING_VALUE = '[missing]'


class FieldCaster(object):
    """Abstract class for casters"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def cast(value):
        """Cast a value
        :param value: the input value
        :type value: str
        :returns: depends on the caster
        """
        pass


class BasicFieldCaster(FieldCaster):
    """Simple implementation of FieldCaster"""
    def __init__(self, cast):
        """Constructor
        :param cast: the cast function
        :type cast: func
        """
        self._cast = cast

    def cast(self, value):
        return self._cast(value)


class MissingValueFieldCaster(FieldCaster):
    """An implementation of FieldCaster that is
    able to handle missing values.
    """
    def __init__(self, cast, options):
        """Constructor
        :param cast: the cast function
        :type cast: func
        :param options: the options to handle missing values.
                        a dict string -> value
                        the keys can be MISSING_OPTION, DEFAULT_OPTION
        """
        self._cast = cast
        self.options = options
        #precompute the value to return in case of missing value
        if MISSING_OPTION in self.options:
            self.missing_value = self._cast(self.options[MISSING_OPTION])
        elif DEFAULT_OPTION in self.options:
            self.missing_value = self._cast(self.options[DEFAULT_OPTION])
        else:
            self.missing_value = self._cast('')

        self.empty_value = None
        if DEFAULT_OPTION in self.options:
            #precompute the value to return in case of empty string
            self.empty_value = self._cast(self.options[DEFAULT_OPTION])

    def cast(self, value):
        if value == MISSING_VALUE:
            return self.missing_value
        elif value == '' and self.empty_value is not None:
            return self.empty_value
        return self._cast(value)


def cast_line_generator(casters):
    """Generates a function that casts a line
    :param casters: the input list of casters
    :type casters: list
    :returns: function - a function that takes a line of string as input
                         and cast each of its element
                         with the appropriate caster.
    """
    lambda_code = "lambda x: ["
    lambda_code += ", ".join(
        ["caster_{}(x[{}])".format(i, i) for i in range(len(casters))]
    )
    lambda_code += "]"

    globals_dict = {}
    for i, caster in enumerate(casters):
        globals_dict["caster_{}".format(i)] = caster
    return eval(lambda_code, globals_dict)


def pad_list(input_list, goal_length, fill_value):
    """Pad elements at the end of a list so that it reach a goal length.
    :param input_list: the input list
    :type input_list: list
    :param goal_length: the size of the returned list
    :type goal_lenght: int
    :param fill_value: the value of the padding elements
    :type fill_value: unknown
    :returns: list"""
    length = len(input_list)
    if length >= goal_length:
        return input_list
    else:
        return input_list + [fill_value] * (goal_length - length)


class Caster(object):
    """
    Cast each field value to an object with respect to a definition mapping in
    *fields*.
    """
    def __init__(self, fields):
        self.casters = []
        for field in fields:
            if len(field) == 2:
                #if the field has size 2, simply apply caster
                name, cast = field
                self.casters.append(cast)
            else:
                name, cast, options = field
                #if the field has size 3, decorate caster so that it can
                #handle missing values
                caster = MissingValueFieldCaster(cast, options)
                self.casters.append(caster.cast)
        self.no_missing_fields = all([len(f) == 2 for f in fields])

    def cast(self, iterable):
        if not self.no_missing_fields:
            #pad MISSING_VALUE if some fields are missing
            iterable = imap(
                lambda x:  pad_list(x, len(self.casters), MISSING_VALUE),
                iterable
            )
        cast_line = cast_line_generator(self.casters)
        return imap(cast_line, iterable)
