import unittest

from cdf.utils.external_sort import (
    external_sort,
    JsonExternalSort,
    MarshalExternalSort,
    PickleExternalSort,
    split_iterable,
    merge_sorted_streams
)


class TestExternalSort(unittest.TestCase):
    def test_nominal_case(self):
        stream = iter([2, 5, 6, 1, 4, 9, 3, 7, 8, 0])
        actual_result = external_sort(stream, lambda x: x)
        self.assertEqual(range(10), list(actual_result))


class TestMergeExternalSort(unittest.TestCase):
    def setUp(self):
        self.stream = iter([2, 5, 6, 1, 4, 9, 3, 7, 8, 0])
        self.block_size = 3
        self.implementations = [
            JsonExternalSort,
            PickleExternalSort,
            MarshalExternalSort
        ]

    def test_all_implementations(self):
        #generate tests for each implementation
        for implementation in self.implementations:
            yield self.check_nominal_case, implementation
            yield self.check_custom_key, implementation
            yield self.check_many_files, implementation

    def check_nominal_case(self, input_class):
        external_sort = input_class(self.block_size)
        actual_result = external_sort.external_sort(self.stream, lambda x: x)
        self.assertEqual(range(10), list(actual_result))

    def check_large_block_size(self, input_class):
        block_size = 100
        external_sort = input_class(block_size)
        actual_result = external_sort.external_sort(self.stream, lambda x: x)
        self.assertEqual(range(10), list(actual_result))

    def check_custom_key(self, input_class):
        external_sort = input_class(self.block_size)
        actual_result = external_sort.external_sort(self.stream, lambda x: -x)
        self.assertEqual(range(9, -1, -1), list(actual_result))

    def check_many_files(self, input_class):
        #test the case where the external sort has to generate many files
        #checks that we do not reach the limit for the number of opened files.
        nb_files = 1018
        stream = xrange(nb_files)
        external_sort = input_class(1)
        actual_result = external_sort.external_sort(stream, lambda x: x)
        self.assertEqual(range(nb_files), list(actual_result))


class TestSplitIterable(unittest.TestCase):
    def setUp(self):
        self.input_stream = xrange(10)

    def test_nominal_case(self):
        actual_result = split_iterable(self.input_stream, 3)
        expected_result = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
        self.assertEqual(expected_result, [list(e) for e in actual_result])

    def test_block_size_equals_stream_size(self):
        actual_result = split_iterable(self.input_stream, 10)
        self.assertEqual([range(10)], [list(e) for e in actual_result])

    def test_large_block_size(self):
        actual_result = split_iterable(self.input_stream, 20)
        self.assertEqual([range(10)], [list(e) for e in actual_result])

    def test_empty_stream(self):
        input_stream = iter([])
        actual_result = split_iterable(input_stream, 10)
        self.assertEqual([], [list(e) for e in actual_result])

    def test_null_block_size(self):
        actual_result = split_iterable(self.input_stream, 0)
        self.assertRaises(
            ValueError,
            list,
            actual_result,
        )


class TestMergeSortedStreams(unittest.TestCase):
    def test_nominal_case(self):
        #even numbers
        stream1 = xrange(1, 10, 2)
        #odd numbers
        stream2 = xrange(0, 10, 2)
        actual_result = merge_sorted_streams([stream1, stream2], lambda x: x)
        self.assertEqual(range(10), list(actual_result))

    def test_custom_key_function(self):
        #even numbers in reverse order
        stream1 = xrange(9, -1, -2)
        #odd numbers in reverse order
        stream2 = xrange(10, -1, -2)
        actual_result = merge_sorted_streams([stream1, stream2], lambda x: -x)
        self.assertEqual(range(10, -1, -1), list(actual_result))

    def test_one_empty_stream(self):
        stream1 = xrange(10)
        stream2 = iter([])
        actual_result = merge_sorted_streams([stream1, stream2], lambda x: x)
        self.assertEqual(range(10), list(actual_result))

    def test_all_empty_streams(self):
        stream1 = iter([])
        stream2 = iter([])
        actual_result = merge_sorted_streams([stream1, stream2], lambda x: x)
        self.assertEqual([], list(actual_result))

    def test_no_stream(self):
        actual_result = merge_sorted_streams([], lambda x: x)
        self.assertEqual([], list(actual_result))


