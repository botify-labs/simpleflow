from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

from simpleflow import storage

from .constants import UNKNOWN_CONTEXT

if TYPE_CHECKING:
    pass


class GetStepsDoneTask:
    """
    List all the steps that are done by parsing
    S3 bucket + path.
    """

    def __init__(self, bucket: str, path: str) -> None:
        self.bucket = bucket
        self.path = path
        self.path_len = len(path) + (1 if not path.endswith("/") else 0)

    def execute(self) -> list[str]:
        steps: list[str] = []
        for f in storage.list_keys(self.bucket, self.path):
            steps.append(f.key[self.path_len :])
        return steps


class MarkStepDoneTask:
    """
    Push a file called `step_name` into bucket/path.
    """

    def __init__(self, bucket: str, path: str, step_name: str) -> None:
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
