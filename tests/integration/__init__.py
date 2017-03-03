import inspect
import os

import boto.swf
from vcr import VCR

import simpleflow.command  # NOQA
from tests.utils import IntegrationTestCase

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
        ("Authorization", "AWS4-HMAC-SHA256 Credential=1234AB/20160823/us-east-1/swf/aws4_request,SignedHeaders=host;x-amz-date;x-amz-target,Signature=foobar"),  # noqa
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
        events = response['events']
        next_page = response.get('nextPageToken')
        while next_page is not None:
            response = self.conn.get_workflow_execution_history(
                self.domain,
                run_id,
                self.workflow_id,
                next_page_token=next_page,
            )

            events.extend(response['events'])
            next_page = response.get('nextPageToken')
        return events
