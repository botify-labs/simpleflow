===============================
Botify CDF : Crawl Data Factory
===============================

Installation
============

Install system dependencies listed in ``packaging/debian_build.deps`` and
``packaging/debian.deps``: ::

    $ cat packaging/debian*.deps | xargs sudo apt-get install

Install ``cdf`` itself: ::

    $ python setup.py install

Or: ::

    $ pip install .

For development: ::

    $ python setup.py develop

Or: ::

    $ pip install -e .
