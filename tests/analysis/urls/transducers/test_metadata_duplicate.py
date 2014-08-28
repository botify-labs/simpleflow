import unittest
from cdf.utils.hashing import string_to_int32
from cdf.analysis.urls.transducers.metadata_duplicate import (
    count_metadata,
    notset_hash_value,
    filter_redundant_metadata,
    preprocess_for_duplicate_computation,
    filter_non_strategic_urls,
    append_zone,
    generate_duplicate_stream
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


class TestFilterRedundantMetatada(unittest.TestCase):
    def test_nominal_case(self):
        fake_hash = 1597530492
        contents_stream = iter([
            (1, 1, fake_hash, "foo title"),
            (1, 2, fake_hash, "foo description"),
            (1, 3, fake_hash, "foo h1"),
            (1, 1, fake_hash, "bar title"),
            (1, 3, fake_hash, "bar h1"),
            (2, 1, fake_hash, "foo title 2"),
            (2, 1, fake_hash, "bar title 2")
        ])
        actual_stream = filter_redundant_metadata(contents_stream)
        expected_stream = [
            (1, 1, fake_hash, "foo title"),
            (1, 2, fake_hash, "foo description"),
            (1, 3, fake_hash, "foo h1"),
            (2, 1, fake_hash, "foo title 2")
        ]
        self.assertEqual(expected_stream, list(actual_stream))


class TestPreprocessForDuplicateComputation(unittest.TestCase):
    def test_non_mandatory_types(self):
        stream_contents = iter((
            [1, 2, 1234, 'My first H1'],
            [1, 3, 7867, 'My H2'],
            [2, 2, 1001, 'My second H1'],
            [2, 5, 1111, 'My H3'],
        ))

        actual_result = preprocess_for_duplicate_computation(stream_contents)

        expected_result = [
            (1, 2, 1234),
            (2, 2, 1001),
        ]
        self.assertEqual(expected_result, list(actual_result))

    def test_notset_metadata(self):
        stream_contents = iter((
            [1, 2, notset_hash_value, ''],
            [2, 2, 1001, 'My second H1'],
        ))

        actual_result = preprocess_for_duplicate_computation(stream_contents)

        expected_result = [
            (2, 2, 1001)
        ]
        self.assertEqual(expected_result, list(actual_result))

    def test_filter_redundant_metadata(self):
        stream_contents = iter((
            [1, 2, 1234, 'My first H1'],
            [1, 4, 7867, 'My description'],
            [1, 2, 2008, 'My second H1'],
            [2, 2, 1001, 'My second H1'],
        ))

        actual_result = preprocess_for_duplicate_computation(stream_contents)

        expected_result = [
            (1, 2, 1234),
            (1, 4, 7867),
            (2, 2, 1001),
        ]
        self.assertEqual(expected_result, list(actual_result))


class TestFilterNonStrategicUrls(unittest.TestCase):
    def test_nominal_case(self):
        contents_stream = iter([
            (1, 1, string_to_int32("title1")),
            (1, 4, string_to_int32("description1")),
            (6, 1, string_to_int32("title1")),
            (6, 4, string_to_int32("description2")),
            (9, 4, string_to_int32("description1"))
        ])

        strategic_urls_stream = iter([
            (1, True, None),
            (6, False, None),
            (9, True, None),
        ])

        actual_result = filter_non_strategic_urls(contents_stream,
                                                  strategic_urls_stream)
        expected_result = [
            (1, 1, string_to_int32("title1")),
            (1, 4, string_to_int32("description1")),
            (9, 4, string_to_int32("description1"))
        ]

        self.assertEqual(expected_result, list(actual_result))


class TestAppendZone(unittest.TestCase):
    def test_nominal_case(self):
        contents_stream = iter([
            (1, 1, string_to_int32("title1")),
            (1, 4, string_to_int32("description1")),
            (6, 1, string_to_int32("title1")),
            (6, 4, string_to_int32("description2")),
            (9, 4, string_to_int32("description1"))
        ])

        zone_stream = iter([
            (1, "en-US,http"),
            (6, "en-US,https"),
            (9, "fr,http"),
        ])

        actual_result = append_zone(contents_stream, zone_stream)

        expected_result = [
            (1, 1, string_to_int32("title1"), "en-US,http"),
            (1, 4, string_to_int32("description1"), "en-US,http"),
            (6, 1, string_to_int32("title1"), "en-US,https"),
            (6, 4, string_to_int32("description2"), "en-US,https"),
            (9, 4, string_to_int32("description1"), "fr,http")
        ]

        self.assertEqual(expected_result, list(actual_result))


class TestGenerateDuplicateStream(unittest.TestCase):
    def test_nominal_case(self):
        contents = [
            (1, 1, string_to_int32("title1")),
            (1, 4, string_to_int32("description1")),
            (2, 1, string_to_int32("title2")),
            (6, 1, string_to_int32("title1")),
            (6, 4, string_to_int32("description2")),
            (8, 4, string_to_int32("description1")),
            (9, 4, string_to_int32("description1"))
        ]
        actual_stream = generate_duplicate_stream(contents, key=lambda x: (x[2], x[1]))

        expected_stream = [
            (1, 1, 2, True, [6]),
            (1, 4, 3, True, [8, 9]),
            (2, 1, 0, True, []),
            (6, 1, 2, False, [1]),
            (6, 4, 0, True, []),
            (8, 4, 3, False, [1, 9]),
            (9, 4, 3, False, [1, 8])
        ]
        #we do not test item order since order is not garanteed by generate_duplicate_stream()
        self.assertItemsEqual(expected_stream, list(actual_stream))

    def test_additional_criterion(self):
        #The last element contains an additional criterion
        contents = [
            (1, 1, string_to_int32("title1"), True),
            (1, 4, string_to_int32("description1"), False),
            (2, 1, string_to_int32("title2"), True),
            (6, 1, string_to_int32("title1"), True),
            (6, 4, string_to_int32("description2"), True),
            (8, 4, string_to_int32("description1"), True),
            (9, 4, string_to_int32("description1"), True)
        ]
        #use the additional criterion in the key function
        actual_stream = generate_duplicate_stream(contents, key=lambda x: (x[2], x[3], x[1]))

        expected_stream = [
            (1, 1, 2, True, [6]),
            (1, 4, 0, True, []),
            (2, 1, 0, True, []),
            (6, 1, 2, False, [1]),
            (6, 4, 0, True, []),
            (8, 4, 2, True, [9]),
            (9, 4, 2, False, [8])
        ]
        #we do not test item order since order is not garanteed by generate_duplicate_stream()
        self.assertItemsEqual(expected_stream, list(actual_stream))


