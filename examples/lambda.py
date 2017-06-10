from simpleflow import Workflow
from simpleflow.lambda_function import LambdaFunction
from simpleflow.swf.task import LambdaFunctionTask

"""
The lambda function is:

from __future__ import print_function

import json

print('Loading function')


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    return 42
"""


class LambdaWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'
    lambda_role = 'arn:aws:iam::111111000000:role/swf-lambda'  # optional, overridable (--lambda-role)

    def run(self):
        future = self.submit(
            LambdaFunctionTask(
                LambdaFunction(
                    'hello-world-python',
                    idempotent=True,
                ),
                8,
                foo='bar',
            )
        )
        print(future.result)
