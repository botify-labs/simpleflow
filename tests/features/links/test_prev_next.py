import unittest
from cdf.features.links.prev_next import compute_prev_next_stream

class TestComputePrevNextStream(unittest.TestCase):
    def test_nominal_case(self):
        inlinks_stream = iter([
            (1, 'a', ["prev"], "", "", ""),
            (2, 'a', [], "", "", ""),
            (3, 'a', ["prev"], "", "", ""),
            (3, 'a', ["prev"], "", "", ""),
            (3, 'a', ["next"], "", "", ""),
        ])

        actual_stream = compute_prev_next_stream(inlinks_stream)

        expected_stream = [
            (1, True, False),
            (2, False, False),
            (3, True, True)
        ]
        self.assertEqual(expected_stream, list(actual_stream))
