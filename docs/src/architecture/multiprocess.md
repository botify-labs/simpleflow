Multiprocess architecture
=========================

!!! note
    This architecture is currently the recommended way to deploy simpleflow in
    production.

If you’re not familiar with the 3 standard roles around an SWF setup, go read the
[previous section about Standalone architecture](standalone/). In this setup, the
3 roles are potentially distributed on different machines:

<div style="text-align:center; padding:15px;">
  <img src="./../../schemas/simpleflow_architecture_multiprocessing.svg" title="Multiprocess Architecture">
</div>

A few notes about this schema:

- the **workflow started** role is generally behind a web app or an automated system depending on your
  use case ; it’s pretty uncommon to have workflows launched manually via the `simpleflow` command-line.
- the **activity workers** can be distributed on many nodes, possibly with *autoscaling* mechanisms.
- the **deciders** on the other hand are usually only installed on a few machines, and don’t need
  autoscaling.
