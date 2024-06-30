# Worker Proxy Support

When deploying a fleet of workers on an instance, their number
may cause the maximum number of connections to SWF to be reached
and let some of these workers unable to fetch activities.

A proxy can be used to prevent this. Simpleflow can now use one,
and provides a simplistic single-threaded proxy.

## Starting the proxy

The `simpleflow proxy.start` command starts an HTTP proxy server.
It processes the CONNECT proxy method; the rest of the connection
is encrypted as before (it doesn't handle HTTPS to MITM the
communication).

## Using the proxy

The `simpleflow worker.start` command accepts a new `-x, --proxy` argument.

## Example

```shell
# Running the `pirate` example with multiple processes.
# Starting the decider, then the workflow, are left as exercises.
[screen 1] $ simpleflow proxy.start
Serving HTTP Proxy on ::1:4242
[screen 2] $ SWF_PROXY=localhost:4242 PYTHONPATH=$PWD SWF_DOMAIN=TestDomain \
 simpleflow worker.start -N 1 -t pirate
```

## Environment setting

Both commands honor a new `SWF_PROXY` environment variable.
