from __future__ import annotations

import abc
import copy
import os
from collections import defaultdict
from typing import TYPE_CHECKING

from simpleflow import activity, settings, task

from .constants import STEP_ACTIVITY_PARAMS_DEFAULT
from .submittable import Step
from .tasks import GetStepsDoneTask

if TYPE_CHECKING:
    from typing import Any, DefaultDict, Sequence


class WorkflowStepMixin(metaclass=abc.ABCMeta):
    def get_step_bucket(self) -> str:
        """
        Return the S3 bucket where to store the steps files.
        """
        return "/".join((settings.SIMPLEFLOW_S3_HOST, settings.STEP_BUCKET))

    def get_step_path_prefix(self) -> str:
        """
        Return the S3 bucket's path prefix where to store the steps files.
        """
        return os.path.join(self.get_run_context().get("workflow_id", "default"), "steps/")

    def get_step_activity_params(self) -> dict[str, Any]:
        """
        Returns extra params for GetStepsDoneTask and MarkStepAsDone activities.
        Will be merged with the default ones.
        """
        return {}

    def add_forced_steps(self, steps: Sequence[str], reason: str | None = None) -> None:
        """
        Add steps to force.
        """
        if not hasattr(self, "steps_forced"):
            self.steps_forced: set[str] = set()
            self.steps_forced_reasons: DefaultDict[str, set] = defaultdict(set)
        steps = set(steps)
        self.steps_forced |= steps
        if reason:
            for step in steps:
                self.steps_forced_reasons[step].add(reason)

    def get_forced_steps(self) -> list[str]:
        """
        Return the list of forced steps.
        """
        return list(getattr(self, "steps_forced", []))

    def add_skipped_steps(self, steps: Sequence[str], reason: str | None = None) -> None:
        """
        Add steps to skip.
        """
        if not hasattr(self, "steps_skipped"):
            self.steps_skipped = set()
            self.steps_skipped_reasons = defaultdict(set)
        steps = set(steps)
        self.steps_skipped |= steps
        if reason:
            for step in steps:
                self.steps_skipped_reasons[step].add(reason)

    def get_skipped_steps(self) -> list[str]:
        """
        Return the list of skipped steps.
        """
        return list(getattr(self, "steps_skipped", []))

    def _get_step_activity_params(self) -> dict[str, Any]:
        """
        Return the merged version between self.get_step_activity_params()
        and the default STEP_ACTIVITY_PARAMS_DEFAULT, plus the workflow
        task list if available.
        """
        activity_params_merged = copy.copy(STEP_ACTIVITY_PARAMS_DEFAULT)
        if hasattr(self, "task_list"):
            activity_params_merged["task_list"] = self.task_list
        activity_params = self.get_step_activity_params()
        if activity_params:
            activity_params_merged.update(activity_params)
        return activity_params_merged

    def step(self, *args: Any, **kwargs: Any) -> Step:
        """
        Return a Step instance.
        """
        return Step(*args, **kwargs)

    def get_steps_done_activity(self) -> task.ActivityTask:
        """
        Return a Submittable returning the list of steps done.
        """
        return task.ActivityTask(
            activity.Activity(GetStepsDoneTask, **self._get_step_activity_params()),
            self.get_step_bucket(),
            self.get_step_path_prefix(),
        )

    def get_steps_done(self) -> list[str]:
        """
        Return the list of steps done.
        """
        return self.submit(self.get_steps_done_activity()).result
