Development
===========


Requirements
------------

- CPython 3.7+
- Pypy 3.7+

The codebase currently needs to be compatible with Python 3.7.
Note about Pypy: all tests pass but some parts of the deciders might not work; Pypy
support is mostly for activity workers where you need the performance boost.


Development environment
-----------------------

A `Dockerfile` is provided to help development on non-Linux machines.

You can build a `simpleflow` image with:

    ./script/docker-build

And use it with:

    ./script/docker-run

It will then mount your current directory inside the container and pass the
most relevant variables (your `AWS_*` credentials for instance).


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


Reproducing Travis failures
---------------------------

It might happen that a test fails on [Travis](https://travis-ci.org/botify-labs/simpleflow)
and you want to reproduce locally. Travis has a [helpful section in their docs](https://docs.travis-ci.com/user/common-build-problems/#Running-a-Container-Based-Docker-Image-Locally)
about reproducing such issues. As of 2022, simpleflow builds run on 20.04 containers on
the Travis infrastructure. So you can get close to the Travis setup with something like:

    docker run -it \
      -u focal \
      -e DEBIAN_FRONTEND=noninteractive \
      -e PYTHONDONTWRITEBYTECODE=true \
      -v $(pwd):/botify-labs/simpleflow \
      quay.io/travisci/travis-python /bin/bash

Then you may want to follow your failed build commands to reproduce the errors.

For instance on pypy builds the commands look like:

    sudo apt-get install ca-certificates libssl1.0.0
    cd botify-labs/simpleflow
    source ~/virtualenv/pypy/bin/activate
    pip install .
    pip install -r requirements-dev.txt
    rm -rf build/
    ./script/test -vv


Release
-------

In order to release a new version, you’ll need credentials on pypi.python.org for this
software, as long as write access to this repository. Ask via an issue if needed.

The release process is then automated behind a script:

    ./script/release
