from simpleflow import logger, Activity
from simpleflow.task import ActivityTask
from simpleflow.utils import import_from_module


def execute_multiple_activities(*args, **kwargs):
    print("execute multiple activities", args, kwargs)
    results = []
    for activity in args[0]:
        funcname = activity["name"].replace("activity-", "")
        logger.info("func is" + funcname)
        func = import_from_module(funcname)
        #logger.info("func is", func)

        if isinstance(func, Activity):
            result = func.callable(*activity["args"], **activity["kwargs"])
        elif isinstance(func, ActivityTask):
            result = func(*activity["args"], **activity["kwargs"]).execute()
        else:
            raise ValueError
        results.append(result)

    return results