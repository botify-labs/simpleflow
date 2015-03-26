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
path=botify/common  # If the path differs from the package's name.
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

Here many fields are set with default values such as `author`, `author_email`,
etc...  Packages are found automatically by `setuptools.find_packages()` behind
the scene. The version is automatically extracted from the module with the
`_version.py` convention.

`botify.common.dist.setup()` adds a `release` command to `python setup.py`:

```
$ python setup.py release -h
Common commands: (see '--help-commands' for more)

  setup.py build      will build the package underneath 'build/'
  setup.py install    will install the package

Global options:
  --verbose (-v)  run verbosely (default)
  --quiet (-q)    run quietly (turns verbosity off)
  --dry-run (-n)  don't actually do anything
  --help (-h)     show detailed help message
  --no-user-cfg   ignore pydistutils.cfg in your home directory

Options for 'Release' command:
  --package       package's name
  --branch        default branch
  --path          path to the top-level of the package
  --url           URL of the project
  --dry-run       dry run
  --version (-v)  version to release
  --major         release major version
  --minor         release minor version
  --micro         release micro version (default)
  --pypi          name (in ~/.pypirc) of the pypi repository to upload to

usage: setup.py [global_opts] cmd1 [cmd1_opts] [cmd2 [cmd2_opts] ...]
   or: setup.py --help [cmd1 cmd2 ...]
   or: setup.py --help-commands
   or: setup.py cmd --help
```

The versioning scheme follow the [Semantic Versioning](https://semver.org) conventions i.e.:

Given a version number `MAJOR.MINOR.PATCH`, increment the:

- `MAJOR` version when you make *incompatible API changes*,
- `MINOR` version when you *add functionality in a backwards-compatible manner*, and
- `PATCH` version when you make *backwards-compatible bug fixes*.

Additional labels for pre-release and build metadata are available as
extensions to the `MAJOR.MINOR.PATCH` format.
