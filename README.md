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
package=botify.common
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

Then edit the `setup.py` file:

```python
##!/usr/bin/env python
# -*- coding: utf-8 -*-

from botify.common.dist import setup


setup(
    name='botify-common',
    description='Botify common utilities',
    namespace_packages=[
        'botify',
        'botify.common'
    ],
)
```

Here many fields are set with default value such as `author`, `author_email`,
etc...  Packages are found automatically by `setuptools.find_packages()` behind
the scene. The version is automatically extracted from the module with the
`_version.py` convention.
