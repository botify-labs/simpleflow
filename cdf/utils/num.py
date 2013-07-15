import ctypes


def to_u64(number):
    return ctypes.c_longlong(number).value
