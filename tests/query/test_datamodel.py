import unittest
from cdf.query.datamodel import get_document_fields_from_features_options


class FieldsTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_enabled(self):
        fields = get_document_fields_from_features_options({"main": {"lang": True}})
        self.assertTrue('lang' in [k[0] for k in fields])

        fields = get_document_fields_from_features_options({"main": {"lang": False}})
        self.assertFalse('lang' in [k[0] for k in fields])

        fields = get_document_fields_from_features_options({"main": None})
        self.assertFalse('lang' in [k[0] for k in fields])
