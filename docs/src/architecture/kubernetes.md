Kubernetes architecture
=======================

!!! warning
    This architecture is currently in alpha mode and should only be used
    at your own risks. It may be deprecated at any time.

This architecture splits activity workers in two parts:

- the **poller** that only polls activity tasks from SWF and boot a corresponding
  Kubernetes job for each activity task.
- the **Kubernetes job** processing an activity task, that doesn’t poll but handle
  the communication with SWF afterward: heartbeats, response when activity is finished.

<div style="text-align:center; padding:15px;">
  <img src="./../../schemas/simpleflow_architecture_kubernetes.svg" title="Kubernetes Architecture">
</div>

There are a few limitations with this design:

- the initial implementation done in [#313](https://github.com/botify-labs/simpleflow/pull/313)
  assumes that pollers are run in a Kubernetes cluster *or* that the decider has a local
  working *kubectl* configuration.
- by design, this architecture doesn’t guarantee that tasks will have a *start timestamp*
  coherent with the moment the task really started to execute. You will get the timestamp
  of the moment the activity was polled from SWF. This may be addressed in the future.
- related to last point: if your cluster doesn't have sufficient resources to schedule
  the jobs, and it waits for too long, you may get a heartbeat (or start to close or
  schedule to close) timeout triggering. So be careful and have your cluster scale as
  needed.
