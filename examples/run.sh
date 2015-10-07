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
