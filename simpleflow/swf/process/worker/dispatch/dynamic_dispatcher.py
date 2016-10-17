# -*- coding: utf-8 -*-
import importlib

from simpleflow.activity import Activity

from .exceptions import DispatchError


class Dispatcher(object):
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
        module_name, activity_name = name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        activity = getattr(module, activity_name, None)
        if not isinstance(activity, Activity):
            raise DispatchError()
        return activity
