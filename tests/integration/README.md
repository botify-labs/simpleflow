# simpleflow integration tests

Integration tests in this directory are meant to hit the real SWF endpoints.
They trigger real API calls, via simpleflow commands, simpleflow functions or
direct boto calls, and check that everything interacts correctly with the SWF
API.

Of course this is not suitable for running the test suite on travis-ci. So the
tests here use [vcrpy](https://vcrpy.readthedocs.io/), a port of the popular
ruby gem: it records HTTP interactions and replay them from the local cache
(called a "cassette") when possible. Cassettes can be refreshed by simply
removing them, which will force real HTTP hits again. Note that for this to
work, you need to have valid SWF credentials for your account on the region
`us-east-1` and the domain `TestDomain` (this is set in `__init__.py` in this
directory).

To sum up:

    # first run: hits SWF APIs, populate the ./cassettes/ directory with
    # serialized resquests/responses
    AWS_DEFAULT_REGION=us-east-1 py.test -v tests/integration/

    # second run (and next): no hit to the SWF APIs are made, requests
    # are replayed from what has been recorded in the first run
    AWS_DEFAULT_REGION=us-east-1 py.test -v tests/integration/

    # if you need to refresh cassettes (a good idea from time to time):
    rm -rf tests/integration/cassettes/*.yaml
    AWS_DEFAULT_REGION=us-east-1 py.test -v tests/integration/

Note that `./script/test` positions a specific environment variable that totally DISABLES
the recording of new cassette files, e.g. it disables HTTP requests to the outside world.
If an HTTP request doesn't match anything in its cassette file, it will then raise an error.
