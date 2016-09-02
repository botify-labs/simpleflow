Changelog
---------

0.11.12
~~~~~~~

- Fix --tags argument for simpleflow standalone (#114)
- Improve tests and add integration tests (#116)
- Add 'simpleflow activity.rerun' command (#117)

0.11.11
~~~~~~~

- Fix a circular import on simpleflow.swf.executor

0.11.10
~~~~~~~

- Fix previous_history initialization (#106)
- Improve WorkflowExecutionQueryset default date values (#111)

0.11.9
~~~~~~

- Add a --repair option to simpleflow standalone (#100)

0.11.8
~~~~~~

- Retry boto.swf connection to avoid frequent errors when using IAM roles (#99)

0.11.7
~~~~~~

Same as 0.11.6 but the 0.11.6 on pypi is broken (pushed something similar to 0.11.5 by mistake)

0.11.6
~~~~~~

- Add issubclass_ method (#96)
- Avoid duplicate logs if root logger has an handler (#97)
- Allow passing SWF domain via the SWF_DOMAIN environment variable (#98)

0.11.5
~~~~~~

- Don't mask activity cancel exception (#84)
- Propagate all decision response attributes up to Executor.replay() (#76, #94)

0.11.4
~~~~~~

- ISO dates in workflow history #91
- Fix potential infinite retry loop #90

0.11.3
~~~~~~

- Fix replay hooks introduced in 0.11.2 (#86)
- Remove python3 compatibility from README (which was not working for a long time)

0.11.2
~~~~~~

- Add new workflow hooks (#79)

0.11.1
~~~~~~

- Fix logging when an exception occurs

0.11.0
~~~~~~

- Merge ``swf`` package into simplefow for easier maintenance.


0.10.4 and below
~~~~~~~~~~~~~~~~

Sorry changes were not documented for simpleflow <= 0.10.x.
