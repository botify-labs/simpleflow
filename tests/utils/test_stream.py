import unittest

from cdf.utils.stream import split_list, split_stream, chunk


class TestSplitList(unittest.TestCase):
    def test_trivial_case(self):
        nb_parts = 2
        actual_result = split_list(range(10), nb_parts)
        expected_result = [range(5), range(5, 10)]
        self.assertEqual(expected_result, [list(g) for g in actual_result])


class TestSplitStream(unittest.TestCase):
    def test_trivial_case(self):
        nb_parts = 2
        stream_size = 10
        actual_result = split_stream(xrange(stream_size), stream_size, nb_parts)
        expected_result = [range(5), range(5, 10)]
        self.assertEqual(expected_result, [list(g) for g in actual_result])

    def test_one_part(self):
        nb_parts = 1
        stream_size = 10
        actual_result = split_stream(xrange(stream_size), stream_size, nb_parts)
        expected_result = [range(10)]
        self.assertEqual(expected_result, [list(g) for g in actual_result])

    def test_null_nb_parts(self):
        nb_parts = 0
        stream_size = 10
        actual_result = split_stream(xrange(stream_size), stream_size, nb_parts)
        self.assertRaises(
            ValueError,
            list,
            actual_result
        )

    def test_variable_size(self):
        nb_parts = 10
        stream_size = 15
        actual_result = split_stream(range(stream_size), stream_size, nb_parts)
        expected_result = [
            [0, 1], [2], [3, 4], [5], [6, 7], [8], [9, 10], [11], [12, 13], [14]
        ]
        self.assertEqual(expected_result, [list(g) for g in actual_result])

        nb_parts = 6
        stream_size = 8
        actual_result = split_stream(range(stream_size), stream_size, nb_parts)
        expected_result = [[0, 1], [2], [3], [4, 5], [6], [7]]
        self.assertEqual(expected_result, [list(g) for g in actual_result])

        nb_parts = 7
        stream_size = 9
        actual_result = split_stream(range(stream_size), stream_size, nb_parts)
        expected_result = [[0, 1], [2], [3], [4, 5], [6], [7], [8]]
        self.assertEqual(expected_result, [list(g) for g in actual_result])

    def test_one_remaining_element(self):
        nb_parts = 5
        stream_size = 6
        actual_result = split_stream(range(stream_size), stream_size, nb_parts)
        expected_result = [[0, 1], [2], [3], [4], [5]]
        self.assertEqual(expected_result, [list(g) for g in actual_result])

    def test_not_enough_elements(self):
        nb_parts = 10
        stream_size = 5
        actual_result = split_stream(range(stream_size), stream_size, nb_parts)
        expected_result = [[0], [1], [2], [3], [4], [], [], [], [], []]
        self.assertEqual(expected_result, [list(g) for g in actual_result])


class TestChunk(unittest.TestCase):
    def test_nominal_case(self):
        stream = iter(range(6))
        chunk_size = 2
        actual_result = chunk(stream, chunk_size)
        expected_result = [[0, 1], [2, 3], [4, 5]]
        self.assertEqual(expected_result, list(actual_result))

    def test_incomplete_chunk(self):
        stream = iter(range(7))
        chunk_size = 2
        actual_result = chunk(stream, chunk_size)
        expected_result = [[0, 1], [2, 3], [4, 5], [6]]
        self.assertEqual(expected_result, list(actual_result))

    def test_big_chunk(self):
        stream = iter(range(4))
        chunk_size = 5
        actual_result = chunk(stream, chunk_size)
        expected_result = [[0, 1, 2, 3]]
        self.assertEqual(expected_result, list(actual_result))

    def test_empty_stream(self):
        stream = iter([])
        chunk_size = 5
        actual_result = chunk(stream, chunk_size)
        expected_result = []
        self.assertEqual(expected_result, list(actual_result))

