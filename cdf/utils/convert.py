import ctypes


def to_int64(number):
    """
    Convert an 64 bits unsigned integer to a 64 bits signed integer
    """
    return ctypes.c_longlong(number).value


def to_int32(number):
    """
    Convert an 64 bits unsigned integer to a 64 bits signed integer
    """
    return ctypes.c_ulong(number).value
