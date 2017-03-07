from .base import Submittable


class WaitForSignal(Submittable):
    """
    Mark the executor must wait on a signal.
    """
    def __init__(self, signal_name, if_new):
        self.signal_name = signal_name
        self.if_new = if_new

    def execute(self):
        pass
