from simpleflow import futures
from simpleflow.canvas import Chain, Group
from simpleflow import Workflow
from simpleflow.exceptions import ExecutionBlocked
from simpleflow.history import History
from simpleflow.swf.executor import Executor
from swf.models.decision import TimerDecision

MY_TIMER = 'my timer'


class BasicWorkflow(Workflow):
    # How to do it without TimerTask
    name = 'basic'
    version = 'example'
    task_list = 'example'

    def run(self, t=30):
        ex = self.executor  # type: Executor
        h = ex._history  # type: History
        # Was MY_TIMER fired?
        my_timers = list(h.swf_history.filter(type='Timer', state='fired', timer_id=MY_TIMER))
        if not my_timers:
            # Was MY_TIMER started?
            if not list(h.swf_history.filter(type='Timer', state='started', timer_id=MY_TIMER)):
                timer = TimerDecision(
                    'start',
                    id=MY_TIMER,
                    start_to_fire_timeout=str(t))
                ex._decisions.append(timer)
                print('Starting timer')
            else:
                print('Timer started, waiting')
            raise ExecutionBlocked
        print('Timer fired, exiting')


class TimerWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'

    def run(self, t1=30, t2=120):
        """
        Cancel timer 2 after timer 1 is fired.
        """
        future = self.submit(
            Group(
                self.start_timer("timer 2", t2),
                Chain(
                    self.start_timer("timer 1", t1),
                    self.cancel_timer("timer 2"),
                ),
            )
        )
        if future.pending:
            print('Starting timers')
        futures.wait(future)
        print('Timer fired, exiting')
