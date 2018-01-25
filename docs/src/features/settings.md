Settings
========

Simpleflow comes with a `simpleflow.settings` module which aims at configuring
generic parameters not tied to a specific simpleflow command.

Parameters can be specified:

- via an environment variable (higher precedence)
- via a python config module, specified by `SIMPLEFLOW_SETTINGS_MODULE`
- via defaults stored in `simpleflow/settings/default.py` (lowest precedence)

For the rest of this document, we will take "SIMPLEFLOW_IDENTITY" as an example. It
controls the identity of SWF deciders and workers as reported to the SWF API when
polling for tasks.


Configure a setting via an environment variable
------------------------------------------------

This is the simplest:
```
$ export SIMPLEFLOW_IDENTITY='{"hostname":"machine.example.net"}'

$ simpleflow info settings | grep IDENTITY
SIMPLEFLOW_IDENTITY='{"hostname":"machine.example.net"}'
```


Configure a setting via a custom module
---------------------------------------

In that form, you will have to create a module that configures the settings
you want first, like this:
```
$ cat my/custom/module.py
SIMPLEFLOW_IDENTITY = '{"hostname":"harcoded.example.net"}'

$ export SIMPLEFLOW_SETTINGS_MODULE='my.custom.module'

$ simpleflow info settings | grep IDENTITY
SIMPLEFLOW_IDENTITY='{"hostname":"harcoded.example.net"}'
```

Of course the example above is not very interesting since the value is
hardcoded, but if you need some settings to be dynamically computed, this
is how you can achieve it.
