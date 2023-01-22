from __future__ import annotations

import copy
from typing import TYPE_CHECKING

from simpleflow import activity
from simpleflow.base import SubmittableContainer
from simpleflow.canvas import Chain, FuncGroup

from .tasks import MarkStepDoneTask
from .utils import (
    get_step_force_reasons,
    get_step_skip_reasons,
    step_is_forced,
    step_is_skipped_by_force,
    step_will_run,
)

if TYPE_CHECKING:
    from typing import Sequence

    from simpleflow.base import Submittable
    from simpleflow.executor import Executor
    from simpleflow.futures import Future


class Step(SubmittableContainer):
    def __init__(
        self,
        step_name: str,
        activities: Submittable | SubmittableContainer,
        force: bool = False,
        activities_if_step_already_done: Submittable | SubmittableContainer | None = None,
        emit_signal: bool = False,
        force_steps_if_executed: Sequence[str] | None = None,
        bubbles_exception_on_failure: bool = False,
    ) -> None:
        """
        :param step_name: Name of the step
        :param activities: submittable entity; not a list
        :param force: Force the step even if already executed
        :param activities_if_step_already_done: submittable to run even step already executed
        :param emit_signal: Emit a signal when the step is executed
        :param force_steps_if_executed: list of steps names to force in the next phases of the workflow
        :param bubbles_exception_on_failure: propagate potential exceptions to the caller
        """
        self.step_name = step_name
        self.activities = activities
        self.force = force
        self.activities_if_step_already_done = activities_if_step_already_done
        self.emit_signal = emit_signal
        self.force_steps_if_executed = force_steps_if_executed or []
        self.bubbles_exception_on_failure = bubbles_exception_on_failure

    def submit(self, executor: Executor) -> Future:
        workflow = executor.workflow
        if workflow is None:
            raise ValueError("Executor has no associated workflow")

        def fn_steps_done(steps_done):
            marker = {
                "step": self.step_name,
                "status": "scheduled",
                "forced": False,
                "reasons": [],
            }
            chain = Chain()
            forced_steps = workflow.get_forced_steps()
            skipped_steps = workflow.get_skipped_steps()
            if step_will_run(self.step_name, forced_steps, skipped_steps, steps_done, self.force):
                if step_is_forced(self.step_name, forced_steps, self.force):
                    marker["forced"] = True
                    marker["reasons"] = get_step_force_reasons(
                        self.step_name, getattr(workflow, "steps_forced_reasons", {})
                    )

                marker_done = copy.copy(marker)
                marker_done["status"] = "completed"

                workflow.add_forced_steps(self.force_steps_if_executed, f"Dep of {self.step_name}")
                chain += (
                    workflow.record_marker("log.step", marker),
                    self.activities,
                    (
                        activity.Activity(MarkStepDoneTask, **workflow._get_step_activity_params()),
                        workflow.get_step_bucket(),
                        workflow.get_step_path_prefix(),
                        self.step_name,
                    ),
                    workflow.record_marker("log.step", marker_done),
                )
            else:
                marker["status"] = "skipped"
                if step_is_skipped_by_force(self.step_name, skipped_steps):
                    marker["forced"] = True
                    marker["reasons"] = get_step_skip_reasons(
                        self.step_name, getattr(workflow, "steps_skipped_reasons", {})
                    )
                else:
                    marker["reasons"] = ["Step was already played"]

                if self.activities_if_step_already_done:
                    chain.append(self.activities_if_step_already_done)
                chain.append(workflow.record_marker("log.step", marker))

            if self.emit_signal:
                chain.append(workflow.signal(f"step.{self.step_name}", propagate=False))
            chain.bubbles_exception_on_failure = self.bubbles_exception_on_failure
            return chain

        return workflow.submit(
            Chain(
                workflow.get_steps_done_activity(),
                FuncGroup(fn_steps_done),
                send_result=True,
            )
        )

    def propagate_attribute(self, attr, val):
        """
        Propagate the attribute to the related activities.
        """
        self.activities.propagate_attribute(attr, val)
