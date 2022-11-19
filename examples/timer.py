from simpleflow import Workflow, futures
from simpleflow.canvas import Chain, Group

MY_TIMER = "my timer"


class TimerWorkflow(Workflow):
    name = "basic"
    version = "example"
    task_list = "example"

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
            print("Starting timers")
        futures.wait(future)
        print("Timer fired, exiting")
