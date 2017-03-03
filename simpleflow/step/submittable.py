from .exceptions import StepNotPreparedException
from .tasks import MarkStepDoneTask

from simpleflow.base import SubmittableContainer
from simpleflow import activity
from simpleflow.canvas import Chain
from .utils import get_step_force_reasons


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

        full_chain = Chain()

        if workflow.step_will_run(self.step_name, self.force):
            marker_msg = '{} is scheduled'.format(self.step_name)
            if workflow.step_is_forced(self.step_name, self.force):
                marker_msg += ' (forced)'
                reasons = get_step_force_reasons(self.step_name, self.steps_force_reasons)
                if reasons:
                    marker_msg += 'Reasons : ' + ', '.join(reasons)

            workflow.add_forced_steps(self.dependencies, 'Dep of {}'.format(self.step_name))
            full_chain += (
                self.activities,
                (activity.Activity(MarkStepDoneTask, **workflow._get_step_activity_params()),
                 workflow.get_step_bucket(),
                 workflow.get_step_path_prefix(),
                 self.step_name),
                workflow.record_marker('log.step', marker_msg)
            )
        else:
            if self.activities_if_step_already_done:
                full_chain.append(self.activities_if_step_already_done)
            full_chain.append(
                workflow.record_marker('log.step', '{} already computed'.format(self.step_name)))

        if self.emit_signal:
            full_chain.append(
                workflow.signal('step.{}'.format(self.step_name), propagate=False))

        return workflow.submit(full_chain)
