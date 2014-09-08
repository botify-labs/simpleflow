import unittest
from cdf.query.constants import FIELD_RIGHTS
from cdf.metadata.url.url_metadata import ES_NO_INDEX
from cdf.core.metadata import make_fields_private


class TestMakeFieldsPrivate(unittest.TestCase):
    def test_nominal_case(self):
        input_mapping = {
            "foo": {
                "verbose_name": "I am foo",
                "settings": set([ES_NO_INDEX, FIELD_RIGHTS.SELECT])
            },
            "bar": {
                "verbose_name": "I am bar",
            }
        }

        actual_result = make_fields_private(input_mapping)
        self.assertEquals(
            set([ES_NO_INDEX, FIELD_RIGHTS.PRIVATE]),
            actual_result["foo"]["settings"]
        )
        self.assertTrue(
            set([FIELD_RIGHTS.PRIVATE]),
            actual_result["bar"]["settings"]
        )
