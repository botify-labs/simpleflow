import unittest

from cdf.analysis.urls.transducers.metadata_duplicate import (
    count_metadata,
    notset_hash_value
)


class TestCountFilledNb(unittest.TestCase):
    def test_nominal_case(self):
        fake_hash = 1597530492

        #metadata for a given urlid are intentionnaly
        #not ordered
        contents_stream = iter([
            (1, 1, fake_hash, "foo title"),
            (1, 2, fake_hash, "foo description"),
            (1, 3, fake_hash, "foo h1"),
            (1, 1, fake_hash, "bar title"),
            (1, 3, fake_hash, "bar h1"),
            (1, 4, notset_hash_value, "not set h2"),
            (2, 1, fake_hash, "foo title 2")
        ])
        part_id = 0
        actual_stream = count_metadata(contents_stream, part_id)

        expected_stream = [
            (1, 1, 2),
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, 1)
        ]
        self.assertEqual(expected_stream, list(actual_stream))
