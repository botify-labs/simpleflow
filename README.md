# Botify Common

## Overview

This library contains common utilities and tools for Botify's projects such as:

- Releasing a project
- Managing settings
- Configuring logging

The first modules focus on release management.

## Quickstart

Add a `setup.cfg` INI file with a `release` section:

```
[release]
package=botify
pypi=botify
url='https://github.com/sem-io/botify-common'
```

Add Python dependencies in `packaging/python.deps`. One dependency by line with
respect to the following formats:

- botify.common
- botify.common==0.0.1
- botify.common<0.0.1
- Or another comparison operator among `<=`, `>=`, `>`.

Add the version in the package main module. For example, `botify.common` is in
the `botify` namespace and its main module is `botify.common`. Hence its main
module is `botify.common` and the version is in the `botify/common/_version.py`
file:

```
VERSION = '0.0.1'
```
