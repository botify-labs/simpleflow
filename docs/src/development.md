Development
===========


Requirements
------------

- Python 2.6.x or 2.7.x
- Python 3.x compatibility is NOT guaranteed for now: https://github.com/botify-labs/simpleflow/issues/87


Development environment
-----------------------

A `Dockerfile` is provided to help development on non-Linux machines.

You can build a `simpleflow` image with:

    ./script/docker-build

And use it with:

    ./script/docker-run

It will then mount your current directory inside the container and pass the
most relevant variables (your AWS_* credentials for instance).


Running tests
-------------

You can run tests with:

    ./script/test

Any parameter passed to this script is propagated to the underlying call to `py.test`.
This wrapper script sets some environment variables which control the behavior of
simpleflow during tests:

- `SIMPLEFLOW_CLEANUP_PROCESSES`: set to `"yes"` in tests, so tests will clean up child
  processes after each test case. You can set it to an empty string (`""`) or omit it if
  outside `script/test` if you want to debug things and take care of it yourself.
- `SIMPLEFLOW_ENV`: set to `"test"` in tests, which changes some constants to ease or
  speed up tests.
- `SWF_CONNECTION_RETRIES`: set to `"1"` in tests, which avoids having too many retries
  on the SWF API calls (5 by default in production).
- `SIMPLEFLOW_VCR_RECORD_MODE`: set to `"none"` in tests, which avoids running requests
  against the real SWF endpoints in tests. If you need to update cassettes, see
  `tests/integration/README.md`


Release
-------

In order to release a new version, you'll need credentials on pypi.python.org for this
software, as long as write access to this repository. Ask via an issue if needed.
Rough process:

    git checkout master
    git pull --rebase
    v=0.10.0
    vi simpleflow/__init__.py
    git add . && git commit -m "Bump version to $v"
    git tag $v
    git push --tags
    python setup.py sdist upload -r pypi
