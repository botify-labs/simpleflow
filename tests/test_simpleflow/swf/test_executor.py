import os
import unittest

import boto
from moto import mock_swf, mock_s3
from sure import expect

from simpleflow import activity, futures
from simpleflow.swf.executor import Executor
from swf.actors import Decider
from simpleflow.swf.process.worker.base import ActivityPoller, ActivityWorker
from swf.models.history import builder
from swf.responses import Response
from tests.data import (
    BaseTestWorkflow,
    DOMAIN,
    increment,
)
from tests.utils import MockSWFTestCase


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
class TestSimpleflowSwfExecutor(MockSWFTestCase):
    def test_submit_resolves_priority(self):
        self.conn.start_workflow_execution("TestDomain", "wfe-1234",
                                           "test-workflow", "v1.2")

        response = Decider(DOMAIN, "test-task-list").poll()
        executor = Executor(DOMAIN, ExampleWorkflow)
        decisions = executor.replay(response).decisions

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


class TestCaseNotNeedingDomain(unittest.TestCase):
    def test_get_event_details(self):
        history = builder.History(ExampleWorkflow, input={})
        signal_input = {'x': 42, 'foo': 'bar', '__propagate': False}
        marker_details = {'baz': 'bae'}
        history.add_signal('a_signal', signal_input)
        history.add_marker('a_marker', marker_details)
        history.add_timer_started('a_timer', 1)
        history.add_timer_fired('a_timer')

        executor = Executor(DOMAIN, ExampleWorkflow)
        executor.replay(Response(history=history, execution=None))

        details = executor.get_event_details('signal', 'a_signal')
        del details['timestamp']
        expect(details).to.equal({
            'type': 'signal',
            'state': 'signaled',
            'name': 'a_signal',
            'input': signal_input,
            'event_id': 4,
            'external_initiated_event_id': 0,
            'external_run_id': None,
            'external_workflow_id': None,
        })

        details = executor.get_event_details('signal', 'another_signal')
        expect(details).to.be.none

        details = executor.get_event_details('marker', 'a_marker')
        del details['timestamp']
        expect(details).to.equal({
            'type': 'marker',
            'state': 'recorded',
            'name': 'a_marker',
            'details': marker_details,
            'event_id': 5,
        })
        details = executor.get_event_details('marker', 'another_marker')
        expect(details).to.be.none

        details = executor.get_event_details('timer', 'a_timer')
        del details['started_event_timestamp']
        del details['fired_event_timestamp']
        expect(details).to.equal({
            'type': 'timer',
            'state': 'fired',
            'id': 'a_timer',
            'start_to_fire_timeout': 1,
            'started_event_id': 6,
            'fired_event_id': 7,
            'control': None,
        })
        details = executor.get_event_details('timer', 'another_timer')
        expect(details).to.be.none


@activity.with_attributes()
def print_me_n_times(s, n):
    return s * n


class ExampleJumboWorkflow(BaseTestWorkflow):
    """
    Example workflow definition used in tests below.
    """
    def run(self, s, n):
        a = self.submit(print_me_n_times, s, n)
        futures.wait(a)
        return a.result


@mock_s3
@mock_swf
class TestSimpleflowSwfExecutorWithJumboFields(MockSWFTestCase):
    def setUp(self):
        os.environ["SIMPLEFLOW_JUMBO_FIELDS_BUCKET"] = "jumbo-bucket"
        self.conn = boto.connect_s3()
        self.conn.create_bucket("jumbo-bucket")
        super(self.__class__, self).setUp()

    def tearDown(self):
        # reset jumbo fields bucket so most tests will have jumbo fields disabled
        os.environ["SIMPLEFLOW_JUMBO_FIELDS_BUCKET"] = ""
        super(self.__class__, self).tearDown()

    def test_jumbo_fields_are_replaced_correctly(self):
        # prepare execution
        self.conn.register_activity_type(
            "TestDomain",
            "tests.test_simpleflow.swf.test_executor.print_me_n_times",
            "default"
        )

        # start execution
        run_id = self.conn.start_workflow_execution(
            "TestDomain", "wfe-1234", "test-workflow", "v1.2",
            input='{"args": ["012345679", 10000]}',
        )["runId"]

        # decider part
        decider = Decider(DOMAIN, "test-task-list")
        response = decider.poll()
        executor = Executor(DOMAIN, ExampleJumboWorkflow)
        result = executor.replay(response)
        assert len(result.decisions) == 1
        decider.complete(response.token,
                         decisions=result.decisions,
                         execution_context=result.execution_context)

        # worker part
        poller = ActivityPoller(DOMAIN, "default")
        response = poller.poll()
        worker = ActivityWorker()
        worker.process(poller, response.task_token, response.activity_task)

        # now check the history
        events = self.conn.get_workflow_execution_history(
            "TestDomain", workflow_id="wfe-1234", run_id=run_id
        )["events"]

        activity_result_evt = events[-2]
        assert activity_result_evt["eventType"] == "ActivityTaskCompleted"
        result = activity_result_evt["activityTaskCompletedEventAttributes"]["result"]

        expect(result).to.match(r"^simpleflow\+s3://jumbo-bucket/[a-z0-9-]+ 90002$")
