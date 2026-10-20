from __future__ import annotations

import abc
import copy
import hashlib
import os
from collections import defaultdict
from typing import TYPE_CHECKING

from simpleflow import activity, constants, settings, task
from simpleflow.utils import json_dumps, retry

from .constants import STEP_ACTIVITY_PARAMS_DEFAULT, STEPS_DONE_MARKER_PREFIX
from .submittable import Step
from .tasks import GetStepsDoneTask, MarkStepDoneTask

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any


class WorkflowStepMixin(metaclass=abc.ABCMeta):
    # Opt-in: list and mark the steps done from the decider instead of
    # GetStepsDoneTask/MarkStepDoneTask activities. The S3 listing is done once
    # per execution and memoized in a marker so that replays stay deterministic.
    steps_in_decider: bool = False

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
            self.steps_forced_reasons: defaultdict[str, set] = defaultdict(set)
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

    def _get_steps_done_in_decider(self) -> list[str]:
        """
        Return the list of steps done, as listed on S3 the first time it is
        needed in this execution.

        The listing is recorded in a marker, then read back from the history on
        the next replays: the steps marked done later in this execution don't
        change the decisions already taken.
        """
        bucket = self.get_step_bucket()
        path = self.get_step_path_prefix()
        location_hash = hashlib.md5(f"{bucket}/{path}".encode()).hexdigest()  # nosec
        marker_name = f"{STEPS_DONE_MARKER_PREFIX}.{location_hash}"

        # Cache for the current replay only: the workflow instance may be reused
        # across replays, whereas the executor builds a new history for each one.
        history = self.executor._history
        if getattr(self, "_steps_done_snapshots", (None,))[0] is not history:
            self._steps_done_snapshots: tuple[Any, dict[str, list[str]]] = (history, {})
        snapshots = self._steps_done_snapshots[1]
        if marker_name not in snapshots:
            event = self.get_event_details("marker", marker_name)
            if event is not None:
                steps_done = event["details"]["steps_done"]
            else:
                steps_done = _execute_with_retry(GetStepsDoneTask(bucket, path))
                details = {"bucket": bucket, "path": path, "steps_done": steps_done}
                if len(json_dumps(details)) > constants.MAX_DETAILS_LENGTH:
                    raise ValueError(
                        f"Too many steps done in {bucket}/{path} to record them in a marker;"
                        " disable steps_in_decider for this workflow"
                    )
                # Not waited for: the marker is recorded along with the other decisions.
                self.submit(self.record_marker(marker_name, details))
            snapshots[marker_name] = steps_done
        return snapshots[marker_name]

    def _mark_step_done_in_decider(self, step_name: str) -> None:
        """
        Push the step file on S3 from the decider.
        """
        mark_step_done = MarkStepDoneTask(self.get_step_bucket(), self.get_step_path_prefix(), step_name)
        run_context = self.get_run_context()
        if run_context.get("run_id"):
            mark_step_done.context = run_context
        _execute_with_retry(mark_step_done)


@retry.with_delay(nb_times=3, delay=retry.exponential)
def _execute_with_retry(a_task: GetStepsDoneTask | MarkStepDoneTask) -> Any:
    # An exception in the decider fails the workflow: retry transient S3 errors.
    return a_task.execute()
