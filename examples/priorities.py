# Simpleflow implements task priorities on SWF, see documentation here:
# http://docs.aws.amazon.com/amazonswf/latest/developerguide/programming-priority.html
#
# Unfortunately boto doesn't support priorities as of version 2.x, so it's
# impossible for simpleflow to implement *default* priorities on SWF objects.
# But it's still possible to schedule tasks with a given priority as this is not
# dependent on arguments on a boto call, but rather passed as data in decisions.
from simpleflow import Workflow, activity


@activity.with_attributes(task_list="quickstart", version="example")
def increment(x):
    return x + 1


class BaseWorkflow(Workflow):
    version = "example"
    task_list = "example"


# EXAMPLE 1: no priority set (equivalent to "0" per the docs)
# Command: simpleflow standalone examples.priorities.WorkflowPriority1 --input '[1]'
class WorkflowPriority1(BaseWorkflow):
    name = "priority-1"

    def run(self, x):
        return self.submit(increment, x).result


# EXAMPLE 2: setting priority task by task
# Command: simpleflow standalone examples.priorities.WorkflowPriority2 --input '[1]'
#
# NB: note that this doesn't really make sense when looking at a single workflow,
# but other workflows may share the same task list with different priorities for
# different tasks.
class WorkflowPriority2(BaseWorkflow):
    name = "priority-2"

    def run(self, x):
        # taskPriority will be set to "5"
        a = self.submit(increment, x, __priority=5)
        # no priority set
        b = self.submit(increment, a)
        return b.result


# EXAMPLE 3: setting a default priority for the workflow
# Command: simpleflow standalone examples.priorities.WorkflowPriority3 --input '[1]'
class WorkflowPriority3(BaseWorkflow):
    name = "priority-3"
    task_priority = 5

    def run(self, x):
        # taskPriority will be set to "5"
        return self.submit(increment, x).result


# EXAMPLE 4: setting a dynamic default priority for the workflow
# Command: simpleflow standalone examples.priorities.WorkflowPriority4 --input '[1]'
class WorkflowPriority4(BaseWorkflow):
    name = "priority-4"

    @property
    def task_priority(self):
        return self._prio

    def run(self, x):
        self._prio = x
        # taskPriority will be set to the value of "x"
        return self.submit(increment, x).result


# EXAMPLE 5: setting a default priority via the @activity.with_attributes() decorator
# Command: simpleflow standalone examples.priorities.WorkflowPriority5 --input '[1]'
#
# This has a higher precedence than the priority set at the workflow level, but
# lower than a "__priority" set in self.submit().
#
# Setting the priority to `None` at a given point results in the activity being
# scheduled without a specific priority (so it takes the default if you
# configured something on SWF, see the docs).
#
# Setting the priority to `simpleflow.activity.PRIORITY_NOT_SET` fallbacks to
# the next priority definition in the precedence list (equivalent to NOT having
# it in the first place). This is advanced usage and you probably don't need
# that.
@activity.with_attributes(task_list="quickstart", version="example", task_priority=12)
def increment_with_high_prio(x):
    return x + 1


class WorkflowPriority5(BaseWorkflow):
    name = "priority-5"
    task_priority = 5

    def run(self, x):
        # priorty will be: 12
        a = self.submit(increment_with_high_prio, x)
        # priorty will be: 13
        b = self.submit(increment_with_high_prio, a, __priority=13)
        # priorty will not be set
        c = self.submit(increment_with_high_prio, b, __priority=None)
        return c.result
