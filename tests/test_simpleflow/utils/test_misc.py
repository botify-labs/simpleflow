import unittest

from simpleflow.utils import format_exc


class MyTestCase(unittest.TestCase):
    def test_format_final_exc_line(self):
        line = None
        try:
            {}[1]
        except Exception as e:
            line = format_exc(e)
        self.assertEqual("KeyError: 1", line)


if __name__ == '__main__':
    unittest.main()
