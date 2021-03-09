Contributing guidelines
=======================

In General
----------

- [PEP 8](http://www.python.org/dev/peps/pep-0008/), when sensible.
- Test ruthlessly. Write docs for new features.
- Even more important than Test-Driven Development, **Human-Driven Development**.


In Particular
-------------

!!! warning
    **THE WHOLE SECTION IS OUT OF DATE.**


**Questions, Feature Requests, Bug Reports, and Feedback**

... should all be reported on the [Github Issue Tracker](https://github.com/botify-labs/simpleflow/issues?state=open).

**Setting Up for Local Development**

1. Fork `simpleflow`_ on Github.
2. Clone your fork::

    $ git clone git@github.com/botify-labs/simpleflow.git

3. Make your virtualenv and install dependencies. If you have virtualenv and virtualenvwrapper_, run::

    $ mkvirtualenv simpleflow
    $ cd simpleflow
    $ pip install -r requirements-dev.txt

- If you don't have virtualenv and virtualenvwrapper, you can install both using `virtualenv-burrito`.


**Git Branch Structure**

simpleflow used to have a separated `devel` branch but is now using only one,
main branch `main`, that contains what will be released in the next version.
This branch is (hopefully) always stable.

**Pull Requests**

1. Create a new local branch. ::

    $ git checkout -b name-of-feature

2. Commit your changes. Write [good commit messages](http://chris.beams.io/posts/git-commit/).

    $ git commit -m "Detailed commit message"
    $ git push origin name-of-feature

3. Before submitting a pull request, check the following:

   - If the pull request adds functionality, it should be tested and the docs should be updated.
   - The pull request should work on Python 2.7 and PyPy. Use `tox` to verify that it does.

4. Submit a pull request to the `main` branch.

**Running tests**

To run all the tests in your current virtual environment: ::

    $ ./script/test

This is what Travis CI does on each environment.

If you want to simulate what Travis CI does, you can approach that by running a container
from them:

    $ ./script/test-travis python2.7
    $ ./script/test-travis pypy

This can help you simulate locally what Travis CI would do. Be aware though that tests may fail depending on your OS, so Travis CI is the reference gate for the project. For instance, installing `subprocess32` in a Pypy environment doesn't work on Mac OSX.
