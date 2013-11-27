import unittest
from nose.tools import assert_equals

import StringIO
from pandas import DataFrame

from cdf.exceptions import InvalidDataFormat
from cdf.utils.loading import build_dataframe_from_csv


class TestDataFrameBuilderFromCsv(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_nominal_case(self):
        csv_file = StringIO.StringIO("Paris\t10000000\nLondon\t8000000\nBerlin\t3000000\n")
        column_names = ["city", "population"]

        dataframe = build_dataframe_from_csv(csv_file, column_names)

        expected_dataframe = DataFrame({
            "city": ["Paris", "London", "Berlin"],
            "population": ["10000000", "8000000", "3000000"]
        })
        assert_equals((3, 2), dataframe.shape)
        assert((column_names == dataframe.columns).all())
        assert((expected_dataframe == dataframe).all().all())

    def test_invalid_input_format(self):

        csv_file = StringIO.StringIO("Paris\t10000000\nLondon\n")
        column_names = ["city", "population"]
        self.assertRaises(InvalidDataFormat,
                          build_dataframe_from_csv,
                          csv_file, column_names)
