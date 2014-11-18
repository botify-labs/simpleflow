import unittest
from cdf.features.links.prev_next import compute_prev_next_stream


class TestComputePrevNextStream(unittest.TestCase):
    def test_nominal_case(self):
        inlinks_stream = iter([
            (1, 'a', ["prev", "follow"], "", "", ""),
            (2, 'a', ["follow"], "", "", ""),
            (3, 'a', ["prev", "follow"], "", "", ""),
            (3, 'a', ["prev", "follow"], "", "", ""),
            (3, 'a', ["next", "follow"], "", "", ""),
        ])

        actual_stream = compute_prev_next_stream(inlinks_stream)

        expected_stream = [
            (1, 1, 0),
            (2, 0, 0),
            (3, 1, 1)
        ]
        self.assertEqual(expected_stream, list(actual_stream))
