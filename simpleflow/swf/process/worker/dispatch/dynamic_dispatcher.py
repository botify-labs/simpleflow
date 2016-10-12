# -*- coding: utf-8 -*-
import importlib


class Dispatcher(object):
    def dispatch(self, name):
        submodule_name, func_name = name.rsplit('.', 1)
        submodule = importlib.import_module(submodule_name)
        return getattr(submodule, func_name)
