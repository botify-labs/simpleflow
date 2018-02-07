import os
import copy
from collections import defaultdict

from .constants import STEP_ACTIVITY_PARAMS_DEFAULT
from .submittable import Step
from .tasks import GetStepsDoneTask
from simpleflow import activity, settings, task


class WorkflowStepMixin(object):

    def get_step_bucket(self):
        """
        Return the S3 bucket where to store the steps files
        """
        return '/'.join((settings.SIMPLEFLOW_S3_HOST, settings.STEP_BUCKET))

    def get_step_path_prefix(self):
        """
        Return the S3 bucket's path prefix where to store the steps files
        """
        return os.path.join(self.get_run_context().get("workflow_id", "default"), 'steps/')

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
            self.steps_forced_reasons = defaultdict(set)
        steps = set(steps)
        self.steps_forced |= set(steps)
        if reason:
            for step in steps:
                self.steps_forced_reasons[step].add(reason)

    def get_forced_steps(self):
        return list(getattr(self, 'steps_forced', []))

    def add_skipped_steps(self, steps, reason=None):
        """
        Add steps to skip
        """
        if not hasattr(self, 'steps_skipped'):
            self.steps_skipped = set()
            self.steps_skipped_reasons = defaultdict(set)
        steps = set(steps)
        self.steps_skipped |= set(steps)
        if reason:
            for step in steps:
                self.steps_skipped_reasons[step].add(reason)

    def get_skipped_steps(self):
        return list(getattr(self, 'steps_skipped', []))

    def _get_step_activity_params(self):
        """
        Returns the merged version between self.get_step_activity_params()
        and the default STEP_ACTIVITY_PARAMS_DEFAULT + workflow task list
        """
        activity_params_merged = copy.copy(STEP_ACTIVITY_PARAMS_DEFAULT)
        if hasattr(self, 'task_list'):
            activity_params_merged["task_list"] = self.task_list
        activity_params = self.get_step_activity_params()
        if activity_params:
            activity_params_merged.update(activity_params)
        return activity_params_merged

    def step(self, *args, **kwargs):
        return Step(*args, **kwargs)

    def get_steps_done_activity(self):
        return task.ActivityTask(activity.Activity(
            GetStepsDoneTask,
            **self._get_step_activity_params()),
            self.get_step_bucket(),
            self.get_step_path_prefix())

    def get_steps_done(self):
        return self.submit(
            self.get_steps_done_activity()).result
