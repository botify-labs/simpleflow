import os
import copy
from collections import defaultdict

from .constants import STEP_ACTIVITY_PARAMS_DEFAULT
from .submittable import Step
from .utils import step_will_run, step_is_forced
from .tasks import GetStepsDoneTask
from simpleflow import activity, settings


class WorkflowStepMixin(object):

    def get_step_bucket(self):
        """
        Return the S3 bucket where to store the steps files
        """
        return settings.SIMPLEFLOW_S3_HOST

    def get_step_path_prefix(self):
        """
        Return the S3 bucket's path prefix where to store the steps files
        """
        return os.path.join(self.get_execution_context().get("workflowId", "default"), 'steps')

    def get_step_activity_params(self):
        """
        Returns the params for GetStepsDoneTask and MarkStepAsDone activities
        Will be merged with the default ones
        """
        return {}

    def add_forced_steps(self, steps, reason=None):
        """
        Add steps to force
        """
        if not hasattr(self, 'steps_forced'):
            self.steps_forced = set()
            self.steps_forced_reasons = defaultdict(list)
        steps = set(steps)
        self.steps_forced |= set(steps)
        if reason:
            for step in steps:
                self.steps_forced_reasons[step].append(reason)

    def get_forced_steps(self):
        return getattr(self, 'steps_forced', [])

    def _get_step_activity_params(self):
        activity_params_merged = copy.copy(STEP_ACTIVITY_PARAMS_DEFAULT)
        activity_params = self.get_step_activity_params()
        if activity_params:
            activity_params_merged.update(activity_params)
        return activity_params_merged

    def step(self, *args, **kwargs):
        return Step(*args, **kwargs)

    def _get_steps_done_future(self):
        if not hasattr(self, '_steps_done_future'):
            self._steps_done_future = self.submit(
                activity.Activity(
                    GetStepsDoneTask,
                    **self._get_step_activity_params()),
                self.get_step_bucket(),
                self.get_step_path_prefix())
        return self._steps_done_future

    def get_steps_done(self):
        return self._get_steps_done_future().result

    def is_step_done(self, step_name):
        return step_name in self.get_steps_done()

    def step_will_run(self, step_name, force=False):
        """
        Return True if step will run by checking :
        1/ force is True
        2/ step_name is in force_steps configuration
        3/ step_name is already computed
        """
        force_steps = self.get_forced_steps()
        steps_done = self.get_steps_done()
        return step_will_run(step_name, force_steps, steps_done, force)

    def step_is_forced(self, step_name, force=False):
        force_steps = self.get_forced_steps()
        return step_is_forced(step_name, force_steps, force)
