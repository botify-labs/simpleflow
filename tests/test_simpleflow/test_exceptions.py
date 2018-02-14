import unittest

from sure import expect

from simpleflow.exceptions import TaskFailed


class TestTaskFailed(unittest.TestCase):
    def test_task_failed_representation(self):
        failure = TaskFailed("message", None, None)
        expect(str(failure)).to.equal("('message', None, None)")
        expect(repr(failure)).to.equal('TaskFailed (message, "None")')

        failure = TaskFailed("message", "reason", "detail")
        expect(str(failure)).to.equal("('message', 'reason', 'detail')")
        expect(repr(failure)).to.equal('TaskFailed (message, "reason")')
