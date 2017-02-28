import json
import copy
import os

from .base import Submittable
from . import storage, futures, activity


ACTIVITY_PARAMS = {
    'schedule_to_start_timeout': 4 * 3600,
    'start_to_close_timeout': 60,
    'schedule_to_close_timeout': 4 * 3600 + 60,
    'heartbeat_timeout': 180,
    'task_priority': 100,
    'version': '1.0',
    'idempotent': True
}
UNKNOWN_CONTEXT = {
    "run_id": "unknown",
    "workflow_id": "unknown",
    "version": "unknown"
}


class StepNotPreparedException(Exception):
    pass


class WorkflowStepMixin(object):

    def prepare_step_config(self, s3_bucket, s3_path_prefix, activity_params={}, force_steps=None):
        activity_params_merged = copy.copy(ACTIVITY_PARAMS)
        activity_params_merged.update(activity_params)
        self._executor.step_config = {
            "s3_bucket": s3_bucket,
            "s3_path_prefix": s3_path_prefix,
            "activity_params": activity_params_merged,
            "force_steps": force_steps or []
        }

    def step(self, *args, **kwargs):
        return Step(*args, **kwargs)


class Step(object):

    def __init__(self, step_name, *activities, **options):
        """
        Register the `activity_group` as a step
        If the step has already been computed in a previous time
        it won't be computed again

        If the step was already computed and `activity_group_if_step_already_done`
        is not empty, we'll call this submittable
        """
        self.step_name = step_name
        self.activities = activities
        self.force = options.pop('force', False)
        self.activities_if_step_already_done = options.pop('activities_if_step_already_done', None)
        self.emit_signal = options.pop('emit_signal', False)

    def submit(self, executor):
        from .canvas import Group, Chain, FuncGroup
        if not hasattr(executor, 'step_config'):
            raise StepNotPreparedException('Please call `workflow.prepare_step_config()` during run')

        path = os.path.join(
            executor.step_config["s3_path_prefix"],
            self.step_name)

        def fn_run_step(steps_done):
            if (self.force or
               should_force_step(self.step_name, executor.step_config["force_steps"]) or
               self.step_name not in steps_done):
                return Chain(
                    Group(self.activities),
                    (activity.Activity(MarkStepDoneTask, **executor.step_config["activity_params"]),
                     executor.step_config["s3_bucket"],
                     path,
                     self.step_name),
                )
            elif self.activities_if_step_already_done:
                return self.activities_if_step_already_done
            return Group()

        chain_step = Chain(send_result=True)
        chain_step.append(
            activity.Activity(GetStepsDoneTask, **executor.step_config["activity_params"]),
            executor.step_config["s3_bucket"],
            executor.step_config["s3_path_prefix"])
        chain_step.append(FuncGroup(fn_run_step))

        full_chain = Chain(chain_step)

        if self.emit_signal:
            full_chain.append(
                executor.signal('step.{}'.format(self.step_name), propagate=False))

        return full_chain.submit(executor)


def should_force_step(step_name, force_steps):
    """
    Check if step_name is in force_steps
    We support multi-level flags, ex for step_name = "a.b.c",
    we allow : "a", "a.b", "a.b.c"
    If one of force_steps is a wildcard (*), it will also force the step
    """
    for step in force_steps:
        if step == "*" or step == step_name or step_name.startswith(step + "."):
            return True
    return False


class GetStepsDoneTask(object):

    def __init__(self, bucket, path):
        self.bucket = bucket
        self.path = path

    def execute(self):
        steps = []
        for f in storage.list_keys(self.bucket, self.path):
            steps.append(f.key[len(self.path):])
        return steps


class MarkStepDoneTask(object):

    def __init__(self, bucket, path, step_name):
        self.bucket = bucket
        self.path = path
        self.step_name = step_name

    def execute(self):
        path = os.path.join(self.path, self.step_name)
        if hasattr(self, 'context'):
            context = self.context
            content = {
                "run_id": context["run_id"],
                "workflow_id": context["workflow_id"],
                "version": context["version"]
            }
        else:
            content = UNKNOWN_CONTEXT
        storage.push_content(self.bucket, path, json.dumps(content))
