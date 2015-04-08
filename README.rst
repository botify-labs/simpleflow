===============================
Botify CDF : Crawl Data Factory
===============================

Installation
============

Install system dependencies listed in ``packaging/debian_build.deps`` and
``packaging/debian.deps``: ::

    $ cat packaging/debian*.deps | xargs sudo apt-get install

Install ``cdf`` itself: ::

    $ python setup.py install

Or: ::

    $ pip install .

For development: ::

    $ python setup.py develop

Or: ::

    $ pip install -e .

Execution of tasks with pypy
============================

 To build the environment install the dependencies defined in
 `packaging/pypy.deps`.

Only a subset of tasks are executed with pypy. They are decorated with
`cdf.utils.execute.with_pypy`.

Settings
========

Some parameters can be set through environment variables such as:

- `BOTIFY_CDF_PYPY_PATH`: the path to the pypy interpreter.
- `BOTIFY_CDF_PYPY_ENABLE`: enable the execution decorated with pypy (True by
                            default).

- `GOOGLE_OAUTH2_KEY`: key to connect to the Google API whe importing data from
                       Google Analytics.
- `GOOGLE_OAUTH2_SECRET`: secret associated with the `GOOGLE_OAUTH2_KEY`.
