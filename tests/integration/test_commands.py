# See README for more informations about integration testsimport sys
import os
import unittest

import boto.swf
from click.testing import CliRunner
from sure import expect

import simpleflow.command
from . import vcr


WORKFLOW_ID = "test-simpleflow-workflow"


class TestSimpleflowCommand(unittest.TestCase):
    @property
    def region(self):
        return os.environ["AWS_DEFAULT_REGION"]

    @property
    def domain(self):
        return os.environ["SWF_DOMAIN"]

    def invoke(self, command, arguments):
        if not hasattr(self, "runner"):
            self.runner = CliRunner()
        return self.runner.invoke(command, arguments.split(" "))

    @property
    def conn(self):
        if not hasattr(self, "_conn"):
            self._conn = boto.swf.connect_to_region(self.region)
        return self._conn

    def cleanup(self):
        # ideally this should be in a tearDown() or setUp() call, but those
        # calls play badly with VCR since they only happen "sometimes"... :-/
        self.conn.terminate_workflow_execution(self.domain, WORKFLOW_ID)

    @vcr.use_cassette
    def test_simpleflow_workflow_start(self):
        """
        Tests simpleflow workflow.start
        """
        # start a workflow
        result = self.invoke(
            simpleflow.command.cli,
            "workflow.start --workflow-id {} --task-list test --input null "
            "--execution-timeout 300 --decision-tasks-timeout 30 "
            "tests.integration.workflow.SleepWorkflow".format(WORKFLOW_ID)
        )

        # check response form: "<workflow id> <run-id>"
        expect(result.exit_code).to.equal(0)
        wf_id, run_id = result.output.split()
        expect(wf_id).to.equal(WORKFLOW_ID)

        # check against SWF that execution is launched
        executions = self.conn.list_open_workflow_executions(
            self.domain, 0, workflow_id=WORKFLOW_ID
        )
        items = executions["executionInfos"]
        expect(len(items)).to.equal(1)
        expect(items[0]["execution"]["runId"]).to.equal(run_id)

        # kill the workflow now
        self.cleanup()

    @vcr.use_cassette
    def test_simpleflow_workflow_terminate(self):
        """
        Tests simpleflow workflow.terminate
        """
        # start a workflow (easier with simpleflow command)
        self.invoke(
            simpleflow.command.cli,
            "workflow.start --workflow-id {} --task-list test --input null "
            "--execution-timeout 300 --decision-tasks-timeout 30 "
            "tests.integration.workflow.SleepWorkflow".format(WORKFLOW_ID)
        )

        # now try to terminate it
        result = self.invoke(
            simpleflow.command.cli,
            "workflow.terminate {} {}".format(self.domain, WORKFLOW_ID),
        )

        # check response form (empty)
        expect(result.exit_code).to.equal(0)
        expect(result.output).to.equal("")


    @vcr.use_cassette
    def test_simpleflow_activity_rerun(self):
        """
        Tests simpleflow activity.rerun
        """
        # run a very short workflow
        result = self.invoke(
            simpleflow.command.cli,
            "standalone --workflow-id %s --input {\"args\":[0]} --nb-workers 1 " \
            "tests.integration.workflow.SleepWorkflow" % WORKFLOW_ID
        )
        expect(result.exit_code).to.equal(0)
        lines = result.output.split("\n")
        start_line = [line for line in lines if line.startswith("test-simpleflow-workflow")][0]
        _, run_id = start_line.split(" ", 1)

        # this workflow has executed a single activity, activity-tests.integration.workflow.sleep-1
        # for which scheduledEventId is 5
        # => let's rerun it locally and check the result
        result = self.invoke(
            simpleflow.command.cli,
            "activity.rerun --workflow-id %s --run-id %s --scheduled-id 5" % (
                WORKFLOW_ID, run_id
            )
        )
        expect(result.exit_code).to.equal(0)
        expect(result.output).to.contain("will sleep 0s")


    # TODO: simpleflow decider.start
    # TODO: simpleflow standalone
    # TODO: simpleflow task.info
    # TODO: simpleflow worker.start
    # TODO: simpleflow workflow.filter
    # TODO: simpleflow workflow.info
    # TODO: simpleflow workflow.list
    # TODO: simpleflow workflow.profile
    # TODO: simpleflow workflow.restart
    # TODO: simpleflow workflow.tasks
