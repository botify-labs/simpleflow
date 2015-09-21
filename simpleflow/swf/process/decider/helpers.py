import swf.models

from simpleflow.swf.executor import Executor


def load_workflow(domain, workflow_name, task_list=None):
    module_name, object_name = workflow_name.rsplit('.', 1)
    module = __import__(module_name, fromlist=['*'])

    workflow = getattr(module, object_name)
    return Executor(swf.models.Domain(domain), workflow, task_list)
