import copy

from .constants import STEP_ACTIVITY_PARAMS_DEFAULT
from .submittable import Step
from .utils import step_will_run, step_is_forced
from .tasks import GetStepsDoneTask
from simpleflow import activity


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
        force_steps = self.step_config["force_steps"]
        steps_done = self.get_steps_done()
        return step_will_run(step_name, force_steps, steps_done, force)

    def step_is_forced(self, step_name, force=False):
        force_steps = self.step_config["force_steps"]
        return step_is_forced(step_name, force_steps, force)
