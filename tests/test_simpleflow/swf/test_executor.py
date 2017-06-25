from sure import expect

from simpleflow import activity, futures
from simpleflow.swf.executor import Executor
from swf.actors import Decider
from swf.models.history import builder
from swf.responses import Response
from tests.data import (
    BaseTestWorkflow,
    DOMAIN,
    TASK_LIST,
    increment,
)
from tests.utils import SimpleflowTestCase


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


class TestSimpleflowSwfExecutor(SimpleflowTestCase):

    def test_submit_resolves_priority(self):
        response = Decider(DOMAIN, TASK_LIST).poll()
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


class TestCaseNotNeedingDomain(unittest.TestCase):
    def test_get_event_details(self):
        history = builder.History(ExampleWorkflow, input={})
        signal_input = {'x': 42, 'foo': 'bar', '__propagate': False}
        marker_details = {'baz': 'bae'}
        history.add_signal('a_signal', signal_input)
        history.add_marker('a_marker', marker_details)
        executor = Executor(DOMAIN, ExampleWorkflow)
        decisions, _ = executor.replay(Response(history=history, execution=None))
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
