import json
import os
from typing import TYPE_CHECKING

from simpleflow import storage

from .constants import UNKNOWN_CONTEXT

if TYPE_CHECKING:
    from typing import AnyStr, List


class GetStepsDoneTask(object):
    """
    List all the steps that are done by parsing
    S3 bucket + path.
    """

    def __init__(self, bucket, path):
        # type: (AnyStr, AnyStr) -> None
        self.bucket = bucket
        self.path = path
        self.path_len = len(path) + (1 if not path.endswith("/") else 0)

    def execute(self):
        # type: () -> List[AnyStr]
        steps = []  # type: List[AnyStr]
        for f in storage.list_keys(self.bucket, self.path):
            steps.append(f.key[self.path_len :])
        return steps


class MarkStepDoneTask(object):
    """
    Push a file called `step_name` into bucket/path.
    """

    def __init__(self, bucket, path, step_name):
        # type: (AnyStr, AnyStr, AnyStr) -> None
        self.bucket = bucket
        self.path = path
        self.step_name = step_name

    def execute(self):
        path = os.path.join(self.path, self.step_name)
        if hasattr(self, "context"):
            context = self.context
            content = {
                "run_id": context["run_id"],
                "workflow_id": context["workflow_id"],
                "version": context["version"],
            }
        else:
            content = UNKNOWN_CONTEXT
        storage.push_content(self.bucket, path, json.dumps(content))
