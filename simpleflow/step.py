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
    'idempotent': True
}


class StepNotPreparedException(Exception):
    pass


class WorkflowStepMixin(object):

    def prepare_step_config(self, s3_uri_prefix, activity_params={}, force_steps=None):
        activity_params_merged = copy.copy(ACTIVITY_PARAMS)
        activity_params_merged.update(activity_params)
        self.step_config = {
            "s3_uri_prefix": s3_uri_prefix,
            "activity_params": activity_params_merged,
            "force_steps": force_steps or []
        }

    def step(self, *args, **kwargs):
        return Step(*args, **kwargs)


class Step(Submittable):

    def __init__(self, step_name, activity_group, force=False, activity_group_if_step_already_done=None,
                 emit_signal=False):
        """
        Register the `activity_group` as a step
        If the step has already been computed in a previous time
        it won't be computed again

        If the step was already computed and `activity_group_if_step_already_done`
        is not empty, we'll call this submittable
        """
        self.step_name = step_name
        self.activity_group = activity_group
        self.force = force
        self.activity_group_if_step_already_done = activity_group_if_step_already_done
        self.emit_signal = emit_signal

    def submit(self, workflow):
        if not hasattr(workflow, 'step_config'):
            raise StepNotPreparedException('Please call `workflow.prepare_step_config()` during run')

        if not hasattr(workflow, 'steps_done'):
            f = workflow.submit(
                activity.Activity(get_steps_done, **workflow.step_config["activity_params"]),
                workflow.step_config["s3_uri_prefix"])
            futures.wait(f)
            workflow.steps_done = f.result

        s3_uri_prefix = self.step_config["s3_uri_prefix"]
        s3_uri = os.path.join(
            s3_uri_prefix,
            step_name)

        signal = workflow.signal('step.{}'.format(self.step_name), propagate=False)
        if (self.force or
           should_force_step(self.step_name, workflow.force_steps) or
           self.step_name not in workflow.steps_done):
            return workflow.submit(Chain(
                activity_group,
                (activity.with_attributes(MarkStepAsDone, **activity_kwargs),
                 s3_uri,
                 self.step_name),
                signal
            ))
        elif activity_group_if_step_already_done:
            return workflow.submit(Chain(
                activity_group_if_step_already_done,
                signal
            ))
        return workflow.submit(signal)


def get_state_path(s3_uri, step_name):
    return os.path.join(s3_uri, 'steps', step_name)


def get_steps_done(s3_uri):
    steps = []
    s3_uri = os.path.join(s3_uri, 'steps')
    bucket, path = storage.get_bucket_and_path_from_uri(s3_uri)
    for f in storage.list_files(bucket, path):
        steps.append(f.uri.split('/')[-1])
    return steps


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


def get_steps_done(uri):
    bucket, path = storage.get_bucket_and_path_from_uri(uri)
    steps = []
    for f in storage.list_keys(bucket, path):
        steps.append(f.split('/')[-1])
    return steps


class MarkStepAsDone(object):

    def __init__(self, s3_uri, step_name):
        self.s3_uri = s3_uri
        self.step_name = step_name

    def execute(self):
        uri = get_state_path(self.s3_uri, self.step_name)
        context = self.context
        content = {
            "run_id": context["run_id"],
            "workflow_id": context["workflow_id"],
            "version": context["version"]
        }
        bucket, path = storage.get_bucket_and_path_from_uri(uri)
        storage.push_content(bucket, path, json.dumps(content))
