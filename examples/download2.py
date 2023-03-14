import subprocess

from simpleflow import Workflow, activity
from simpleflow.log import colorize


@activity.with_attributes(task_list="quickstart", version="example")
def use_custom_binary(args):
    return subprocess.check_output(args)


class BasicWorkflow(Workflow):
    name = "basic"
    version = "example"
    task_list = "example"

    def run(self):
        binary = "how-is-simpleflow"
        location_tpl = "s3://botify-labs-simpleflow/binaries/{version}/how_is_simpleflow"

        # This shows how to map a dynamic binary version to an activity task.
        # Of course, the example is far-fetched ;-)
        for version in ["v1", "v2", "latest"]:
            use_custom_binary.meta["binaries"] = {binary: location_tpl.format(version=version)}
            msg = self.submit(use_custom_binary, [binary]).result
            print(colorize("BLUE", msg))


# Run with:
# LOG_LEVEL=info simpleflow standalone --nb-deciders 1 --nb-workers 1 examples.download2.BasicWorkflow --input '[]' 2>&1
