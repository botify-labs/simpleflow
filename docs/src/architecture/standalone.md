Standalone mode
===============

!!! warning
    This mode is well-suited for development but not for production, since
    it uses the local machine alone to take decisions and process activity
    tasks.

In this mode, your local machine will act as:

- **workflow starter**: it submits the workflow to Amazon SWF (via the `StartWorkflowExecution`
  endpoint)
- **decider**: it polls SWF for decisions to take (via `PollForDecisionTask`) then takes
  decisions with the workflow class you passed and submit decisions to SWF
  (via `RespondDecisionTaskCompleted`)
- **activity worker**: it polls SWF for activity tasks to execute (via `PollForActivityTask`)
  then processes the task and submit the result to SWF, either via `RespondActivityTaskCompleted`
  if successful, or via `RespondActivityTaskFailed` if not.


Usually you’ll want to tune the number of decider and worker processes that
will be spawned by the command. The command will look like:
```bash
simpleflow standalone --domain TestDomain \
    --nb-deciders 1 --nb-workers 1 \
    examples.basic.BasicWorkflow --input '[1]'
```

Your process tree will look like:
```
/code/simpleflow# ps auxf|grep simpleflow
PID     COMMAND
  6 ... simpleflow standalone examples.basic.BasicWorkflow --input [1]
 10 ...  \_ simpleflow Decider(payload=DeciderPoller.start, nb_children=1)[running]
 12 ...  |   \_ simpleflow DeciderPoller(task_list=basic-1234)[processing]
 14 ...  |       \_ simpleflow DeciderPoller(task_list=basic-1234)[deciding]
 11 ...  \_ simpleflow Worker(payload=ActivityPoller.start, nb_children=1)[running]
 13 ...      \_ simpleflow ActivityPoller(task_list=basic-1234)[polling]
```

Under your command, you now see 2 subprocesses:

- a **Decider supervisor** process, that forks to one or more **DeciderPoller** processes
- a **Worker supervisor** process, that forks to one or more **ActivityPoller** processes

Each poller then forks when doing real work related to SWF. Here we’re in the middle of
a decision for the workflow, and no activity task is running.
