def _raw_to_bool(string):
    return string == '1' or string == 1 or string is True


def str_to_int_list(string):
    return [int(p) for p in string.split()]
