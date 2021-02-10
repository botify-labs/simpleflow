#!/bin/bash

echo "BASIC WORKFLOW"
(
  set -x
  simpleflow workflow.start --local examples.basic.BasicWorkflow --input '[3, 0]'
)

echo
echo "FAILING WORKFLOW"
(
  set -x
  simpleflow workflow.start --local examples.failing.FailingWorkflow --input '[]' 2>&1
) | sed "/^ .*/d;s/^Traceback.*/<...snip...>/"
echo
echo "WORKFLOW WITH MIDDLEWARE"
(
  set -x
  simpleflow workflow.start --local examples.basic.BasicWorkflow --input '[3, 0]' \
    --middleware-pre-execution examples.middleware.my_pre_execution_middleware \
    --middleware-post-execution examples.middleware.my_post_execution_middleware
)
