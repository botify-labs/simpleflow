from simpleflow.activity import Activity
from simpleflow.utils import import_object_from_module

from .exceptions import DispatchError


class Dispatcher:
    """
    Dispatch by name, like simpleflow.swf.process.worker.dispatch.by_module.ModuleDispatcher
    but without a hierarchy.
    """

    @staticmethod
    def dispatch_activity(name):
        """

        :param name:
        :type name: str
        :return:
        :rtype: Activity
        :raise DispatchError: if doesn't exist or not an activity
        """
        module_name, activity_name = name.rsplit(".", 1)
        try:
            activity = import_object_from_module(module_name, activity_name)
        except ImportError:
            # We were not able to import a function at all.
            raise DispatchError(f"unable to import '{name}'")
        if not isinstance(activity, Activity):
            # We managed to import a function (or callable) but it's not an
            # "Activity". We will transform it into an Activity now. That way
            # we can accept functions that are *not* decorated with
            # "@activity.with_attributes()" or equivalent. This dispatcher is
            # used in the context of an activity worker, so we don't actually
            # care if the task is decorated or not. We only need the decorated
            # function for the decider (options to schedule, retry, fail, etc.).
            activity = Activity(activity, activity_name)
        return activity
