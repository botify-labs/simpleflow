import os
import json

from simpleflow import storage
from .constants import UNKNOWN_CONTEXT


class GetStepsDoneTask(object):
    """
    List all the steps that are done by parsing
    S3 bucket + path
    """

    def __init__(self, bucket, path):
        self.bucket = bucket
        self.path = path

    def execute(self):
        steps = []
        for f in storage.list_keys(self.bucket, self.path):
            steps.append(f.key[len(self.path) + 1:])
        return steps


class MarkStepDoneTask(object):
    """
    Push a file called `step_name` into bucket/path
    """

    def __init__(self, bucket, path, step_name):
        self.bucket = bucket
        self.path = path
        self.step_name = step_name

    def execute(self):
        path = os.path.join(self.path, self.step_name)
        if hasattr(self, 'context'):
            context = self.context
            content = {
                "run_id": context["run_id"],
                "workflow_id": context["workflow_id"],
                "version": context["version"]
            }
        else:
            content = UNKNOWN_CONTEXT
        storage.push_content(self.bucket, path, json.dumps(content))
