from __future__ import print_function

import os
from simpleflow import (
    activity,
    futures,
    Workflow,
)
from simpleflow.canvas import Group
from simpleflow.step.workflow import WorkflowStepMixin
from simpleflow.step.submittable import Step


@activity.with_attributes(task_list='example', version='example', idempotent=True)
def multiply(*numbers):
    val = 1
    for n in numbers:
        val *= n
    return val


class StepWorkflow(Workflow, WorkflowStepMixin):
    # This workflow demonstrates the use of simpleflow's Step submittable
    #
    # The second execution time of the workflow will skip the step 'my_step'
    # Because it waw already computed before, thanks to a file put on S3

    name = 'step'
    version = 'example'
    task_list = 'example'

    def run(self):
        future = self.submit(
            Step(
                'my_step',
                Group(
                    (multiply, 1),
                    (multiply, 2),
                    (multiply, 3),
                )
            )
        )
        futures.wait(future)

        # You can force the step even if already executed
        group = Group((multiply, 1), (multiply, 2), (multiply, 3))
        step = Step(
            'my_step_force',
            group,
            force=True)
        futures.wait(self.submit(step))

        # You can play another activity group in the step was already computed
        group_done = Group(self.signal('DONE'))
        step = Step(
            'my_step_with_callback_done',
            group,
            activities_if_step_already_done=group_done)
        futures.wait(self.submit(step))

        # You can emit a signal with the identifier step.{step_name}
        # after the step is executed (cached or not)
        step = Step(
            'my_step_with_signal',
            group,
            emit_signal=True)
        futures.wait(self.submit(step))


class CustomizedStepWorkflow(Workflow, WorkflowStepMixin):
    # You can customize the place where the steps files are located by overriding the following methods:

    def get_step_bucket(self):
        # It can be a S3 Bucket name
        bucket = "my_bucket"
        # In case the bucket is not in US-east, you can prepend the s3 host endpoint
        # in the bucket return
        # List available here : http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
        bucket = "s3-eu-west-1.amazonaws.com/my_bucket"
        return bucket

    def get_step_path_prefix(self):
        # The prefix where to put the steps files
        # For example we have a workflow storing data for a person
        # We want to locate it in a specific directory
        return os.path.join('people', self.people_id, 'steps')

    def get_step_activity_params(self):
        # We have 2 tasks defined by simpleflow for the step module :
        # 1/ GetStepsDoneTask -> list the steps which are done
        # 2/ MarkStepDoneTask -> mark a step as done
        # By default the params of those activities are simpleflow.step.constants.STEP_ACTIVITY_PARAMS_DEFAULT
        # and the default workflows's task list
        # But you can return a dict that will update this default dict
        # (you don't need to provide all the keys)
        return {
            "task_list": "specific_task_list"
        }

    def run(self, people_id, force_steps=None):
        self.people_id = people_id

        # You can declare step forcing
        # at workflow initialization
        # it can comes from the context or the result
        # of a specific activity result
        if force_steps:
            self.add_forced_steps(force_steps,
                                  reason="workflow_init")

        # Forcing of steps are multi-leveled
        # Ex : if you force "a.b", you will force all steps
        # named "a.b" or starting by "a.b." (ex : a.b.c)
        self.add_forced_steps(["a.b"])

        # You can also force all steps by calling "*"
        self.add_forced_steps("*")
