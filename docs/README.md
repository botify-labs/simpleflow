Simpleflow documentation
========================

This directory hosts simpleflow documentation.


Installing
----------

The following commands will install the python libraries needed
for developing the on the docs website. You may want to activate
a `virtualenv` before running the commands:

    pip install pip-tools
    ./script/pip-sync


Running for development
-----------------------

This command launches a live development server that is accessible
on http://localhost:9000/.

    ./script/run

Note that the server will reload automatically on file changes in
the `docs/` folder, _but_ it won't refresh assets included via the
markdown-include extension (namely the "README.md" section in the
"Intro" page, and the "LICENSE" page).


Deploying
---------

This command builds the full static site and pushes the result
to the `gh-pages` branch on Github, which makes the doc available
at https://botify-labs.github.io/simpleflow/.

    ./script/deploy

Support for https://simpleflow.readthedocs.io/ may be reintroduced
later.
