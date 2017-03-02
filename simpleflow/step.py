import json
import copy
import os

from .base import SubmittableContainer
from .canvas import Group, Chain, FuncGroup
from . import storage, activity


STEP_ACTIVITY_PARAMS_DEFAULT = {
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

    def prepare_step_config(self, s3_bucket, s3_path_prefix, activity_params=None, force_steps=None):
        activity_params_merged = copy.copy(STEP_ACTIVITY_PARAMS_DEFAULT)
        if activity_params:
            activity_params_merged.update(activity_params)
        self.step_config = {
            "s3_bucket": s3_bucket,
            "s3_path_prefix": s3_path_prefix,
            "activity_params": activity_params_merged,
            "force_steps": force_steps or []
        }
        self._steps_done_future = self.submit(
            activity.Activity(
                GetStepsDoneTask,
                **self.step_config["activity_params"]),
            self.step_config["s3_bucket"],
            self.step_config["s3_path_prefix"])

    def step(self, *args, **kwargs):
        return Step(*args, **kwargs)

    def get_steps_done(self):
        return self._steps_done_future.result

    def is_step_done(self, step_name):
        return step_name in self.get_steps_done()

    def step_will_run(self, step_name, force=False):
        """
        Return True if step will run by checking :
        1/ force is True
        2/ step_name is in force_steps configuration
        3/ step_name is already computed
        """
        return (
            force or
            should_force_step(step_name, self.step_config["force_steps"]) or
            self.is_step_done(step_name))


class Step(SubmittableContainer):

    def __init__(self, step_name, activities, force=False, activities_if_step_already_done=None,
                 emit_signal=False, dependencies=None):
        """
        :param step_name : Name of the step
        :param force : Force the step even if already executed
        :param activities_if_step_already_done : Activities to run even step already executed
        :param emit_signal : Emit a signal when the step is executed
        :param dependencies : list of steps name to force afterward
        """
        self.step_name = step_name
        self.activities = activities
        self.force = force
        self.activities_if_step_already_done = activities_if_step_already_done
        self.emit_signal = emit_signal
        self.dependencies = dependencies or []

    def submit(self, executor):
        workflow = executor.workflow
        if not hasattr(workflow, 'step_config'):
            raise StepNotPreparedException('Please call `workflow.prepare_step_config()` during run')

        path = os.path.join(
            workflow.step_config["s3_path_prefix"],
            self.step_name)

        full_chain = Chain()

        if workflow.step_will_run(self.step_name, self.force):
            workflow.step_config["force_steps"] += self.dependencies
            full_chain += (
                self.activities,
                (activity.Activity(MarkStepDoneTask, **workflow.step_config["activity_params"]),
                 workflow.step_config["s3_bucket"],
                 path,
                 self.step_name),
            )
        elif self.activities_if_step_already_done:
            full_chain += self.activities_if_step_already_done

        if self.emit_signal:
            full_chain.append(
                workflow.signal('step.{}'.format(self.step_name), propagate=False))

        return workflow.submit(full_chain)


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
    """
    List all the steps that are done by parsing
    S3 bucket + path
    """

    def __init__(self, bucket, path):
        self.bucket = bucket
        self.path = path

    def execute(self):
        steps = []
        for f in storage.list_keys(self.bucket, self.path):
            steps.append(f.key[len(self.path):])
        return steps


class MarkStepDoneTask(object):
    """
    Push a file called `step_name` into bucket/path
    """

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
