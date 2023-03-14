from __future__ import annotations

from .base import Submittable


class WaitForSignal(Submittable):
    """
    Mark the executor must wait on a signal.
    """

    def __init__(self, signal_name):
        self.signal_name = signal_name

    def execute(self):
        pass
