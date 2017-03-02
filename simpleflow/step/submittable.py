from .exceptions import StepNotPreparedException
from .tasks import MarkStepDoneTask

from simpleflow.base import SubmittableContainer
from simpleflow import activity
from simpleflow.canvas import Chain


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

        full_chain = Chain()

        if workflow.step_will_run(self.step_name, self.force):
            marker_msg = '{} is scheduled'.format(self.step_name)
            if workflow.step_is_forced(self.step_name, self.force):
                marker_msg += ' (forced)'
            workflow.record_marker('step.log', marker_msg)

            workflow.step_config["force_steps"] += self.dependencies
            full_chain += (
                self.activities,
                (activity.Activity(MarkStepDoneTask, **workflow.step_config["activity_params"]),
                 workflow.step_config["s3_bucket"],
                 workflow.step_config["s3_path_prefix"],
                 self.step_name),
            )
        else:
            workflow.record_marker('step.log', '{} already computed'.format(self.step_name))
            if self.activities_if_step_already_done:
                full_chain.append(self.activities_if_step_already_done)

        if self.emit_signal:
            full_chain.append(
                workflow.signal('step.{}'.format(self.step_name), propagate=False))

        return workflow.submit(full_chain)
