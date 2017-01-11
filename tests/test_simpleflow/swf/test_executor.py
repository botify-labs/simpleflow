import unittest

import boto
from moto import mock_swf
from sure import expect

from simpleflow import activity, futures
from simpleflow.swf.executor import Executor
from swf.actors import Decider
from tests.data import (
    BaseTestWorkflow,
    DOMAIN,
    increment,
)


@activity.with_attributes(task_priority=32)
def increment_high_priority(self, x):
    return x + 1


class ExampleWorkflow(BaseTestWorkflow):
    """
    Example workflow definition used in tests below.
    """
    @property
    def task_priority(self):
        """
        Sets a default task priority as a dynamic value. We could also have used
        task_priority = <num> on the class directly.
        """
        return 12

    def run(self):
        a = self.submit(increment, 3)
        b = self.submit(increment, 3, __priority=5)
        c = self.submit(increment, 3, __priority=None)
        d = self.submit(increment_high_priority, 3)
        e = self.submit(increment_high_priority, 3, __priority=30)
        futures.wait(a, b, c, d, e)


@mock_swf
class TestSimpleflowSwfExecutor(unittest.TestCase):
    def setUp(self):
        self.conn = boto.connect_swf()
        self.conn.register_domain("TestDomain", "50")
        self.conn.register_workflow_type(
            "TestDomain", "test-workflow", "v1.2",
            task_list="test-task-list", default_child_policy="TERMINATE",
            default_execution_start_to_close_timeout="6",
            default_task_start_to_close_timeout="3",
        )
        self.conn.start_workflow_execution("TestDomain", "wfe-1234",
                                           "test-workflow", "v1.2")

    def tearDown(self):
        pass

    def test_submit_resolves_priority(self):
        response = Decider(DOMAIN, "test-task-list").poll()
        executor = Executor(DOMAIN, ExampleWorkflow)
        decisions, _ = executor.replay(response)

        expect(decisions).to.have.length_of(5)

        def get_task_priority(decision):
            return decision["scheduleActivityTaskDecisionAttributes"].get("taskPriority")

        # default priority for the whole workflow
        expect(get_task_priority(decisions[0])).to.equal("12")

        # priority passed explicitly
        expect(get_task_priority(decisions[1])).to.equal("5")

        # priority == None
        expect(get_task_priority(decisions[2])).to.be.none

        # priority set at decorator level
        expect(get_task_priority(decisions[3])).to.equal("32")

        # priority set at decorator level but overridden in self.submit()
        expect(get_task_priority(decisions[4])).to.equal("30")
