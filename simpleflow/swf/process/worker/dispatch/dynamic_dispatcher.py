# -*- coding: utf-8 -*-
import importlib


class Dispatcher(object):
    """
    Dispatch by name, like simpleflow.swf.process.worker.dispatch.by_module.ModuleDispatcher
    but without a hierarchy.
    """
    def dispatch_activity(self, name):
        """

        :param name:
        :type name: str
        :return:
        :rtype: simpleflow.activity.Activity
        """
        module_name, activity_name = name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        return getattr(module, activity_name)
