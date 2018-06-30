import abc

from future.utils import with_metaclass


class Submittable(object):
    """
    Object directly submittable to an executor, without wrapping:
    E.g. an ActivityTask but not an Activity.
    """
    def propagate_attribute(self, attr, val):
        pass


class SubmittableContainer(with_metaclass(abc.ABCMeta)):
    """
    Object where submission returns either Submittable or SubmittableContainer objects

    We cannot pass those objects directly to the executor
    """
    def propagate_attribute(self, attr, val):
        pass

    @abc.abstractmethod
    def submit(self, executor):
        pass
