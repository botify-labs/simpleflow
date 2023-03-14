from __future__ import annotations

import inspect
import os
from typing import TYPE_CHECKING

import boto.swf  # noqa
from click.testing import CliRunner
from sure import expect
from vcr import VCR

import simpleflow.command
from simpleflow.utils import json_dumps
from tests.utils import IntegrationTestCase

if TYPE_CHECKING:
    from click.testing import Result


# Default SWF parameters
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["SWF_DOMAIN"] = "TestDomain"


# VCR config with better defaults for SWF API calls
def test_name_to_cassette_path(function):
    name = function.__name__
    directory = os.path.dirname(inspect.getfile(function))
    return os.path.join(directory, "cassettes", name + ".yaml")


vcr = VCR(
    func_path_generator=test_name_to_cassette_path,
    filter_headers=[
        (
            "Authorization",
            (
                "AWS4-HMAC-SHA256 Credential=1234AB/20160823/us-east-1/swf/"
                "aws4_request,SignedHeaders=host;x-amz-date;x-amz-target,Signature=foobar"
            ),
        ),  # noqa
    ],
    record_mode=os.getenv("SIMPLEFLOW_VCR_RECORD_MODE", "once"),
)


# Base class for integration tests
WORKFLOW_ID = "test-simpleflow-workflow"


class VCRIntegrationTest(IntegrationTestCase):
    @property
    def region(self):
        return os.environ["AWS_DEFAULT_REGION"]

    @property
    def domain(self):
        return os.environ["SWF_DOMAIN"]

    @property
    def workflow_id(self):
        return WORKFLOW_ID

    @property
    def conn(self):
        if not hasattr(self, "_conn"):
            self._conn = boto.swf.connect_to_region(self.region)
        return self._conn

    def get_events(self, run_id):
        response = self.conn.get_workflow_execution_history(
            self.domain,
            run_id,
            self.workflow_id,
        )
        events = response["events"]
        next_page = response.get("nextPageToken")
        while next_page is not None:
            response = self.conn.get_workflow_execution_history(
                self.domain,
                run_id,
                self.workflow_id,
                next_page_token=next_page,
            )

            events.extend(response["events"])
            next_page = response.get("nextPageToken")
        return events

    def invoke(self, arguments: str | list[str], catch_exceptions: bool = True) -> Result:
        if not hasattr(self, "runner"):
            self.runner = CliRunner()
        if isinstance(arguments, str):
            arguments = arguments.split(" ")
        return self.runner.invoke(simpleflow.command.cli, arguments, catch_exceptions=catch_exceptions)

    def run_standalone(self, workflow_name, *args, **kwargs):
        input = json_dumps(dict(args=args, kwargs=kwargs))
        result = self.invoke(
            [
                "standalone",
                "--workflow-id",
                str(self.workflow_id),
                "--input",
                input,
                "--nb-deciders",
                "2",
                "--nb-workers",
                "2",
                workflow_name,
            ],
        )
        expect(result.exit_code).to.equal(0)
        lines = result.output.split("\n")
        start_line = [line for line in lines if line.startswith(self.workflow_id)][0]
        _, run_id = start_line.split(" ", 1)

        events = self.get_events(run_id)
        return events
