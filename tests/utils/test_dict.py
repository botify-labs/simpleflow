import unittest

from cdf.utils.dict import update_path_in_dict, flatten_dict, deep_dict


class TestDictUtils(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_update_path(self):
        # all empty
        _dict = {}
        _path = 'a.b.c'
        _value = 1

        expected = {'a': {'b': {'c': 1}}}
        update_path_in_dict(_path, _value, _dict)
        self.assertDictEqual(expected, _dict)

        # add a sub-path to existing dict
        _dict = {'a': {'d': 2}}
        _path = 'a.b.c'
        _value = 1

        expected = {'a': {'b': {'c': 1}, 'd': 2}}
        update_path_in_dict(_path, _value, _dict)
        self.assertDictEqual(expected, _dict)

        # override existing value
        _dict = {'a': {'b': {'c': 2}}}
        _path = 'a.b.c'
        _value = 10

        expected = {'a': {'b': {'c': 10}}}
        update_path_in_dict(_path, _value, _dict)
        self.assertDictEqual(expected, _dict)

        # add value among existing values
        _dict = {'a': {'b': {'d': 100, 'f': 20}}}
        _path = 'a.b.c'
        _value = 1

        expected = {'a': {'b': {'c': 1, 'd': 100, 'f': 20}}}
        update_path_in_dict(_path, _value, _dict)
        self.assertDictEqual(expected, _dict)

    def test_flatten(self):
        _dict = {'a': {'b': {'d': 100, 'f': 20}}}
        expected = {'a.b.d': 100, 'a.b.f': 20}
        self.assertDictEqual(flatten_dict(_dict), expected)

        _dict = {'a.b.c': 100}
        self.assertDictEqual(flatten_dict(_dict), _dict)

    def test_deep_dict(self):
        _dict = {'a.b.d': 100, 'a.b.f': 20}
        expected = {'a': {'b': {'d': 100, 'f': 20}}}
        self.assertDictEqual(deep_dict(_dict), expected)