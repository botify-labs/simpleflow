from __future__ import annotations

import multiprocessing
import os
import signal
import time

from sure import expect

from simpleflow.command import start_workflow
from simpleflow.swf import helpers
from simpleflow.swf.process import decider, worker
from tests.integration import VCRIntegrationTest, vcr


class TestTaskLists(VCRIntegrationTest):
    @vcr.use_cassette
    def test_not_standalone(self):
        decider_proc = multiprocessing.Process(
            target=decider.command.start,
            args=(
                [
                    "tests.integration.workflow.ChainTestWorkflow",
                    "tests.integration.workflow.TestRunChild",
                ],
                self.domain,
                None,
            ),
            kwargs={
                "nb_processes": 1,
                "repair_with": None,
                "force_activities": False,
                "is_standalone": False,
            },
        )
        decider_proc.start()

        worker_proc = multiprocessing.Process(
            target=worker.command.start,
            args=(
                self.domain,
                "quickstart",
            ),
            kwargs={
                "nb_processes": 1,
                "heartbeat": 10,
            },
        )
        worker_proc.start()

        ex = start_workflow.callback(
            "tests.integration.workflow.TestRunChild",
            self.domain,
            self.workflow_id,
            task_list=None,
            execution_timeout="10",
            tags=None,
            decision_tasks_timeout="10",
            input="[]",
            input_file=None,
            local=False,
            middleware_pre_execution=None,
            middleware_post_execution=None,
        )
        while True:
            time.sleep(1)
            ex = helpers.get_workflow_execution(
                self.domain,
                ex.workflow_id,
                ex.run_id,
            )
            if ex.status == ex.STATUS_CLOSED:
                break

        expect(ex.status).to.equal(ex.STATUS_CLOSED)
        expect(ex.close_status).to.equal(ex.CLOSE_STATUS_COMPLETED)
        os.kill(worker_proc.pid, signal.SIGTERM)
        worker_proc.join()
        os.kill(decider_proc.pid, signal.SIGTERM)
        decider_proc.join()
