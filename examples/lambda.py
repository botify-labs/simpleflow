from simpleflow import Workflow, futures
from simpleflow.lambda_function import LambdaFunction
from simpleflow.swf.task import LambdaFunctionTask


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
        print(futures.wait(future))
