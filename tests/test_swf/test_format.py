import unittest
import random

import swf.format
import swf.constants


class TestFormat(unittest.TestCase):
    def test_wrap_none(self):
        self.assertEquals(
            swf.format.wrap(None, 1),
            None
        )

    def test_wrap_smaller(self):
        MAX_LENGTH = random.randint(10, 1000)
        message = 'A' * (MAX_LENGTH // 2)
        self.assertEquals(
            swf.format.wrap(message, MAX_LENGTH),
            message,
        )

    def test_wrap_longer(self):
        MAX_LENGTH = random.randint(10, 1000)
        message = 'A' * 1000
        self.assertEquals(
            len(swf.format.wrap(message, MAX_LENGTH)),
            MAX_LENGTH,
        )

    def test_reason(self):
        message = 'A' * (swf.constants.MAX_REASON_LENGTH * 2)
        self.assertEquals(
            len(swf.format.reason(message)),
            swf.constants.MAX_REASON_LENGTH,
        )

    def test_details(self):
        message = 'A' * (swf.constants.MAX_DETAILS_LENGTH * 2)
        self.assertEquals(
            len(swf.format.details(message)),
            swf.constants.MAX_DETAILS_LENGTH,
        )

    def test_input(self):
        message = 'A' * (swf.constants.MAX_INPUT_LENGTH * 2)
        self.assertEquals(
            len(swf.format.input(message)),
            swf.constants.MAX_INPUT_LENGTH,
        )

    def test_result(self):
        message = 'A' * (swf.constants.MAX_RESULT_LENGTH * 2)
        self.assertEquals(
            len(swf.format.result(message)),
            swf.constants.MAX_RESULT_LENGTH,
        )

    def test_execution_context(self):
        message = 'A' * (swf.constants.MAX_RESULT_LENGTH * 2)
        self.assertEquals(
            len(swf.format.execution_context(message)),
            swf.constants.MAX_EXECUTION_CONTEXT_LENGTH,
        )

    def test_heartbeat_details(self):
        message = 'A' * (swf.constants.MAX_RESULT_LENGTH * 2)
        self.assertEquals(
            len(swf.format.heartbeat_details(message)),
            swf.constants.MAX_HEARTBEAT_DETAILS_LENGTH,
        )

    def test_identity(self):
        message = 'A' * (swf.constants.MAX_RESULT_LENGTH * 2)
        self.assertEquals(
            len(swf.format.identity(message)),
            swf.constants.MAX_IDENTITY_LENGTH,
        )
