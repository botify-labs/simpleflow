Changelog
=========

0.21.18
-------



0.21.18rc2
----------



0.21.18rc1
----------

- (Merge branch 'enhancement/403/Canvas_misc_enhancements' into main)
- (Merge branch 'enhancement/399/execute.python_add_env_argument' into main)
- (Merge branch 'enhancement/397/decider.start_work_without_workflows' into main)
- (Merge branch 'enhancement/395/workflow.filter_add_start_close_timestamps' into main)

0.21.17.post1
-------------



0.21.17
-------

-  (#394)
- Replace `master` with `main` in release script (#389)

0.21.16
-------

- Add middleware (#380)
- Enhancement/386/seasonal cleanups (#385)
- Fix #378 (#379)
- Add executable path to swf identity (#384)
- Add __main__.py (#382)

0.21.15
-------

- Improvement/375/task failure context add full exception class name (#377)

0.21.14a3
---------



0.21.14a2
---------



0.21.14a1
---------

- Enhancement/367/improve release script v2 (#376)
- Bugfix/update dependencies (#373)

0.21.13
-------



0.21.12
-------

- Update subprocess32 to 3.5.x (#366)

0.21.11
-------

- task/340/remote-syslog-support (#345)

0.21.10
-------

- task/DATA-7023/local_executor_run_id (#362)
- Enhancement 364: add ChildWorkflowTask class (#365)
- canvas: support WorkflowTask in the Group() canvas (#360)

0.21.9
------

- Prune worker process tree. (#355)

0.21.8
------

- swf: WorkflowTask: add the possibility to use a custom task list (#356)
- tests/moto_compat.py: new file (#358)
- Add BOTO_CONFIG=/dev/null to scripts/test (#357)
- spelling fix, failuer->failure (#353)

0.21.7
------

- Fix reading requirements-dev.txt again (#352)

0.21.6
------

- Fix reading requirements-dev.txt :roll_eyes: (#351)

0.21.5
------

- Remove explicit handling of AWS credentials from env vars. (#347)
- Enhancement: use moto 1.x (#349)
- Add optional S3 server-side encryption (#350)

0.21.4
------



0.21.3
------

- Don't try creating ActivityType's multiple times (#342)

0.21.2
------



0.21.1
------

- simpleflow.execute.python: handle huge args (#339)

0.21.0
------

- Feature: custom logic on retry (#332)

0.20.8
------

- Expose simpleflow.utils.serialize_complex_object() function (#336)

0.20.7
------

- Feature: raises_on_failure and retry on workflow (#334)

0.20.6
------

- Improve tests and fix jumbo fields decoding on failure (#330)
- Remove future.standard_library.install_aliases() (#329)

0.20.5
------

- Add 'simpleflow binaries.download' command

0.20.4
------

- Fail activity task on k8s job spawning failure (#328)

0.20.3
------

- Small settings improvements (#323)

0.20.2
------

- add meta to metrology, upload stats only at the end of a task (#324)

0.20.1
------

- inject context into python subprocess (#322)

0.20.0
------

- Feature/318/simpleflow download binary (#321)

0.19.2
------

- Slow jumbo cache fixes (#320)

0.19.1
------



0.19.0
------

- Kubernetes integration (#313)

0.18.15
-------

- Bugfix: propagate signal (#312)
- Enhancement: inherit tag list (#314)
- * blank SWF decision execution context when needed
* rename ambiguous "execution_context" to "run_context" (#310)

0.18.14
-------

- Update the link to the documentation (#306)
- Fork on each decision task to protect against memory leaks (#200) (#308)
- Don't truncate too long fields, raise instead (closes #307) (#309)

0.18.13
-------

- Fix diskcache OperationalError (#303)

0.18.12
-------

- Enhancement: save waiting_signals in the execution context (#300)
- Mark flaky tests as expected to fail (#301)

0.18.11
-------

- Allow Workflow instances in Group (#299)

0.18.10
-------

- Don't raise when ThrottlingException occur on RecordActivityTaskHeartbeat endpoint (#297)

0.18.9
------

- Fix activity.rerun not working with class based tasks (#289)
- Add a new option (and parameter) --kill-children (#292)

0.18.8
------

- Move workers cleanup/start outside SIGCHLD handler (#290)

0.18.7
------

- Fix MANIFEST.in so README.md is included in final package

0.18.6
------

- Add a new timeout parameter (#286)

0.18.5
------

- Documentation overhaul (#284)
- Add a release script (closes #179) (#287)

0.18.4
------

- Improve process stopping (#283)

0.18.3
------

- Enhancement/276/improve execute python (#280)

0.18.2
------

- Bugfix: task failed details (#281)
- Add sets support to json_dumps (#275)

0.18.1
------

- Add back get_workflow_history


0.18.0
------

- Implement "jumbo" fields (#265)

0.17.0
------

- Enhancement/272/implement workflow cancelation (#273)
- Bugfix: 270: signals improvements (#271)
- Enhancement: timer: get_event_details (#269)
- Append "/" to get_step_path_prefix (#268)
- Enhancement/misc (#266)
- Repair reruns successful child workflows (#191)

0.16.0
------

- Feature: timers (#258)

0.15.7
------

- Kill worker on UnknownResourceFault's during a heartbeat (#88) (#263)
- Sort keys by default in json_dumps (#264)

0.15.6
------

- Fix step attribute propagation (#261)
- Enhancement: get_event_details (#235)

0.15.5
------

- Enhancement: distinguish raises_on_failure between tasks and groups (#255)
- Add time constants
- Relax activity.with_attributes timeouts types

0.15.4
------

- Enhancement: add canvas option break_on_failure (#253)
- Compute task_id from ActivityTask if has get_task_id method (#237)
- Another case of wrong task list (#234)

0.15.3
------

- make raises_on_failure=True on step activities (#249)
- SWF: support for non-Python tasks (#219)
- Fix get_step_path_prefix
- Make MarkerTask's idempotents

0.15.2
------

- mark when a step is scheduled before it's executed (#243)

0.15.1
------

- Enhancement: better activity type reason (#238)
- Fix workers not catching errors during dispatch() step (#246)
- Fix canvas.Chain send_result regression (#247)

0.15.0
------

- Feature: steps (#221)
- Make activity task result optional (#225)
- Use details in addition to name to find markers (#227)
- Logging: add exception information (#163)
- swf/actors: support 'Message' key (#224)
- Implement markers (#216) (#217)
- Add retry on swf.process.Poller.poll and fail (#208)

0.14.2
------

- propagate_attribute: skip signal objects (#215)
- Local executor: check add_activity_task (#215)

0.14.1
------

- Don't send exception up if raises_on_failure is false (#213)
- Fix UnicodeDecodeError on windows machine (#211)
- Try to use less memory (#209)
- Standalone mode: use created task list for children activities (#207)

0.14.0
------

- Fix workers not stopping in case they start during a shutdown (#205)
- Add support for SWF signals (#188)
- Improvements on canvas.Group (#204)

0.13.4
------

- Implement metrology on SWF and local workflows (#186)

0.13.3
------

- Try..except pass for NoSuchProcess (#182)

0.13.2
------

- Add optional canvas (#193)
- Reorganize tests/ directory (#198)
- Relax DeciderPoller task list check (#201)
- Implement priorities on SWF tasks (#199)

0.13.1
------

- Fix SWF executor not accepting ActivityTask's in submit() method (#196)

0.13.0
------

- Implement child workflow (#74)
- Don't schedule idempotent tasks multiple times (#107)
- Child workflow ID: use parent's id to generate

0.12.7
------

- Control SWF processes identity via environment (#184)

0.12.6
------

- Replace `execution` object with a more flexible `get_execution_method()` (#177)
- Fix README_SWF.rst format (#175)
- Fix CONTRIBUTING.rst format
- docs/conf.py: remove relative import

0.12.5
------

- Executor: expose workflow execution (#172)

0.12.4
------

- Avoid returning too big responses to RespondDecisionTaskCompleted endpoint (#166)
- Worker: remove useless monitor_child (#168)

0.12.3
------

- Add max_parallel option in Group (#164)

0.12.2
------

- Make the dynamic dispatcher more flexible (#161)
- Fix README.rst format (#160)
- Tiny command-line usability fixes (#158)

0.12.1
------

- Don't override passed "default" in json_dumps() (#155)
- Expose activity context (#156)

0.12.0
------

- Improve process management (#142)

0.11.17
-------

- Don't reap children in the back of multiprocessing (#141)
- Don't force to pass a workflow to activity workers (#133)
- Don't override the task list if not standalone (#139)
- Split FuncGroup submit (#146)
- CI: Test on python 3 (#144)
- Decider: use workflow's task list if unset (#148)

0.11.16
-------

- Refactor: cleanups and many python 3 compatibility issues fixed (#135)
- Introduce AggregationException to inspect exceptions inside canvas.Group/Chain (#92)
- Improve heartbeating, now enabled by default on activity workers (#136)

0.11.15
-------

- Fix tag_list declaration in case no tag is associated with the workflow
- Fix listing workflow tasks not handling "scheduled" (not started) tasks correctly
- Fix CSV formatter outputing an extra "None" at the end of the output
- Fix 'simpleflow activity.rerun' resolving the bad function name if not the last event

0.11.14
-------

- Various little fixes around process management, heartbeat, logging (#110)

0.11.13
-------

- Add ability to provide a 'run ID' with 'simpleflow standalone --repair'

0.11.12
-------

- Fix --tags argument for simpleflow standalone (#114)
- Improve tests and add integration tests (#116)
- Add 'simpleflow activity.rerun' command (#117)

0.11.11
-------

- Fix a circular import on simpleflow.swf.executor

0.11.10
-------

- Fix previous_history initialization (#106)
- Improve WorkflowExecutionQueryset default date values (#111)

0.11.9
------

- Add a --repair option to simpleflow standalone (#100)

0.11.8
------

- Retry boto.swf connection to avoid frequent errors when using IAM roles (#99)

0.11.7
------

Same as 0.11.6 but the 0.11.6 on pypi is broken (pushed something similar to 0.11.5 by mistake)

0.11.6
------

- Add `issubclass_` method (#96)
- Avoid duplicate logs if root logger has an handler (#97)
- Allow passing SWF domain via the SWF_DOMAIN environment variable (#98)

0.11.5
------

- Don't mask activity cancel exception (#84)
- Propagate all decision response attributes up to Executor.replay() (#76, #94)

0.11.4
------

- ISO dates in workflow history #91
- Fix potential infinite retry loop #90

0.11.3
------

- Fix replay hooks introduced in 0.11.2 (#86)
- Remove python3 compatibility from README (which was not working for a long time)

0.11.2
------

- Add new workflow hooks (#79)

0.11.1
------

- Fix logging when an exception occurs

0.11.0
------

- Merge `swf` package into simplefow for easier maintenance.


0.10.4 and below
----------------

Sorry changes were not documented for simpleflow <= 0.10.x.
