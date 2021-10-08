import json

from simpleflow import logger
from simpleflow.utils import import_from_module


def execute_multiple_activities(*args, **kwargs):
    print("execute multiple activities", args, kwargs)
    results = []
    for activity in args:
        funcname = activity["name"].replace("activity-", "")
        logger.info("func is", funcname)
        func = import_from_module()
        logger.info("func is", func)

        if isinstance(func, object):
            result = func(activity["args"], activity["kwargs"]).run()
        else:
            result = func(activity["args"], activity["kwargs"])
        results.append(result)

    return results