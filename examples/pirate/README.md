Example Pirate Business Workflow
================================

This example workflow demonstrates basic features of simpleflow in a fun way.

It can be executed like this:
```
export AWS_DEFAULT_REGION=eu-west-1
export SWF_DOMAIN=TestDomain
simpleflow standalone \
  --nb-deciders 1 \
  --nb-workers 2 \
  --input '{"kwargs":{"money_needed": 120}}' \
  examples.pirate.decider.PirateBusiness
```
