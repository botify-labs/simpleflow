import copy

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


class Step(SubmittableContainer):
    def __init__(
        self,
        step_name,
        activities,
        force=False,
        activities_if_step_already_done=None,
        emit_signal=False,
        force_steps_if_executed=None,
        bubbles_exception_on_failure=False,
    ):
        """
        :param step_name : Name of the step
        :param activities : submittable entity, not a list.
        :type activities : Submittable | SubmittableContainer
        :param force : Force the step even if already executed
        :param activities_if_step_already_done : Activities to run even step already executed
        :param emit_signal : Emit a signal when the step is executed
        :param force_steps_if_executed : list of steps names to force in the next phases of the workflow
        :param bubbles_exception_on_failure : propagate potential exceptions to the caller
        """
        self.step_name = step_name
        self.activities = activities
        self.force = force
        self.activities_if_step_already_done = activities_if_step_already_done
        self.emit_signal = emit_signal
        self.force_steps_if_executed = force_steps_if_executed or []
        self.bubbles_exception_on_failure = bubbles_exception_on_failure

    def submit(self, executor):
        workflow = executor.workflow

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
            if step_will_run(
                self.step_name, forced_steps, skipped_steps, steps_done, self.force
            ):
                if step_is_forced(self.step_name, forced_steps, self.force):
                    marker["forced"] = True
                    marker["reasons"] = get_step_force_reasons(
                        self.step_name, getattr(workflow, "steps_forced_reasons", {})
                    )

                marker_done = copy.copy(marker)
                marker_done["status"] = "completed"

                workflow.add_forced_steps(
                    self.force_steps_if_executed, "Dep of {}".format(self.step_name)
                )
                chain += (
                    workflow.record_marker("log.step", marker),
                    self.activities,
                    (
                        activity.Activity(
                            MarkStepDoneTask, **workflow._get_step_activity_params()
                        ),
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
                chain.append(
                    workflow.signal("step.{}".format(self.step_name), propagate=False)
                )
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
