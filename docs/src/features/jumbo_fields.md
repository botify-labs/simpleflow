Jumbo Fields
============

!!! warning
    This feature is in _beta_ mode and subject to changes. Any feedback is appreciated.

For some use cases, you want to be able to have fields larger than what SWF accepts
(which is maximum 32K bytes on the largest ones, `input` and `result`, and lower for
some others, as documented [here](http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dg-limits.html)).

Simpleflow allows to transparently translate such fields to objects stored on AWS
S3. The format is then the following:

    simpleflow+s3://jumbo-bucket/with/optional/prefix/5d7191af-[...]-cdd39a31ba61 5242880


Format
------

The format provides a pseudo-S3 address as a first word. The `simpleflow+s3://`
prefix is here for implementation purposes, and may be extended later with other
backends such as `simpleflow+ssh` or `simpleflow+gs`.

The second word provides the length of the object in bytes, so a client parsing
the SWF history can decide if it’s worth it to pull/decode the object.

For now jumbo fields are limited to 5MB in size.

Simpleflow will optionally perform disk caching for this feature to avoid
issuing too many queries to S3. The disk cache is enabled if you set the
`SIMPLEFLOW_ENABLE_DISK_CACHE` environment variable. The resulting disk
cache will be limited to 1GB, with an LRU eviction strategy. It uses
Sqlite3 under the hood, and it’s powered by the
[DiskCache library](http://www.grantjenks.com/docs/diskcache/).
Note that this cache used to be enabled by default, but it’s not anymore,
since it proved to slow things down under certain circumstances that we
couldn’t track down precisely.


Configuration
-------------

You have to configure an environment variable to tell simpleflow where to store
things (which implicitly enables the feature by the way):

    SIMPLEFLOW_JUMBO_FIELDS_BUCKET=jumbo-bucket/with/optional/prefix

And ensure your deciders and activity workers have access to this S3 bucket (`s3:GetObject` and
`s3:PutObject` should be enough, but please test it first).

!!! warning "Warning on bucket name length"
    The overhead of the signature format is maximum 91 chars at this point (fixed protocol
    and UUID width, and max 5M = 5242880 for the size part). So you should ensure
    that your bucket + directory is not longer than 256 - 91 = 165 chars, else
    you may not be able to get a working jumbo field signature for tiny fields.
    In that case stripping the signature would only break things down the road
    in unpredictable and hard to debug ways, so simpleflow will raise.
