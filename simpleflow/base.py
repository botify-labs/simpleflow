import abc
from typing import TYPE_CHECKING

from future.utils import with_metaclass

if TYPE_CHECKING:
    from simpleflow.executor import Executor  # NOQA


class Submittable(object):
    """
    Object directly submittable to an executor, without wrapping:
    E.g. an ActivityTask but not an Activity.
    """

    def propagate_attribute(self, attr, val):
        pass


class SubmittableContainer(with_metaclass(abc.ABCMeta)):
    """
    Objects where submission returns either Submittable or SubmittableContainer objects

    We cannot pass those objects directly to the executor
    """

    def propagate_attribute(self, attr, val):
        pass

    @abc.abstractmethod
    def submit(self, executor):
        # type: (Executor) -> None
        pass
