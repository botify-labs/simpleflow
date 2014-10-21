import unittest

from cdf.utils.dict import (
    update_path_in_dict,
    flatten_dict,
    deep_dict,
    update_dict,
    delete_path_in_dict,
    path_in_dict
)


class TestUpdatePath(unittest.TestCase):
    def test_empty_dict(self):
        # all empty
        _dict = {}
        _path = 'a.b.c'
        _value = 1

        expected = {'a': {'b': {'c': 1}}}
        update_path_in_dict(_path, _value, _dict)
        self.assertDictEqual(expected, _dict)

    def test_subpath_addition(self):
        # add a sub-path to existing dict
        _dict = {'a': {'d': 2}}
        _path = 'a.b.c'
        _value = 1

        expected = {'a': {'b': {'c': 1}, 'd': 2}}
        update_path_in_dict(_path, _value, _dict)
        self.assertDictEqual(expected, _dict)

    def test_override_existing_value(self):
        _dict = {'a': {'b': {'c': 2}}}
        _path = 'a.b.c'
        _value = 10

        expected = {'a': {'b': {'c': 10}}}
        update_path_in_dict(_path, _value, _dict)
        self.assertDictEqual(expected, _dict)

    def test_existing_value_addition(self):
        # add value among existing values
        _dict = {'a': {'b': {'d': 100, 'f': 20}}}
        _path = 'a.b.c'
        _value = 1

        expected = {'a': {'b': {'c': 1, 'd': 100, 'f': 20}}}
        update_path_in_dict(_path, _value, _dict)
        self.assertDictEqual(expected, _dict)

    def test_top_level_update(self):
        _dict = {'a': 1}
        _path = 'a'
        _value = 2

        expected = {'a': 2}
        update_path_in_dict(_path, _value, _dict)
        self.assertDictEqual(expected, _dict)

    def test_no_update(self):
        # won't update a non-dict element
        _dict = {'a': 1}
        _path = 'a.b'
        _value = 2

        expected = {'a': 1}  # not {'a': {'b': 2}}
        update_path_in_dict(_path, _value, _dict)
        self.assertDictEqual(expected, _dict)


class TestFlattenDict(unittest.TestCase):
    def test_nominal_case(self):
        _dict = {'a': {'b': {'d': 100, 'f': 20}}}
        expected = {'a.b.d': 100, 'a.b.f': 20}
        self.assertDictEqual(flatten_dict(_dict), expected)

    def test_flat_dict(self):
        _dict = {'a.b.c': 100}
        self.assertDictEqual(flatten_dict(_dict), _dict)


class TestDeepDict(unittest.TestCase):
    def test_nominal_case(self):
        _dict = {'a.b.d': 100, 'a.b.f': 20}
        expected = {'a': {'b': {'d': 100, 'f': 20}}}
        self.assertDictEqual(deep_dict(_dict), expected)


class TestUpdateDict(unittest.TestCase):
    def test_nominal_case(self):
        _dict = {'a': {'b': {'c': 1}}, 'k': 3}
        update = {'a': {'e': 2}, 'k': 5}
        expected = {'a': {'e': 2, 'b': {'c': 1}}, 'k': 5}

        update_dict(_dict, update)
        self.assertDictEqual(_dict, expected)


class TestDeletePath(unittest.TestCase):
    def setUp(self):
        self._dict = {'a': {'b': {'c': 1, 'd': [1, 2]}}}

    def test_nominal_case(self):
        path = 'a.b.c'
        expected = {'a': {'b': {'d': [1, 2]}}}
        delete_path_in_dict(path, self._dict)
        self.assertEqual(self._dict, expected)

    def test_length_one_path(self):
        path = 'a'
        expected = {}
        delete_path_in_dict(path, self._dict)
        self.assertEqual(self._dict, expected)

    def test_unexisting_path(self):
        # `path` does not exist
        path = 'd.k'
        expected = {'a': {'b': {'c': 1, 'd': [1, 2]}}}
        delete_path_in_dict(path, self._dict)
        self.assertDictEqual(self._dict, expected)


class TestPathInDict(unittest.TestCase):
    def setUp(self):
        self._dict = {'a': {'b': {'c': 1, 'd': [1, 2]}}}

    def test_positive_case(self):
        self.assertTrue(path_in_dict("a.b.c", self._dict))

    def test_negative_case(self):
        self.assertFalse(path_in_dict("a.z", self._dict))

    def test_short_path(self):
        self.assertTrue(path_in_dict("a.b", self._dict))

    def test_too_long_path(self):
        self.assertFalse(path_in_dict("a.b.c.e", self._dict))
