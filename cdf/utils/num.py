import struct


def to_u64(number):
    return struct.unpack('q', struct.pack('Q', number))[0]
