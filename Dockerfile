FROM ubuntu:14.04

# 1- PREPARE SYSTEM
# -----------------

# This forces python to have stdout/sdtin/stderr totally unbuffered, which is
# better for logs especially, and recommended in most Django Dockerfiles I've
# seen.
ENV PYTHONUNBUFFERED 1

# Don't ask for interactive confirmation on install/updates
ENV DEBIAN_FRONTEND noninteractive

# Path to the directory that hosts Python virtualenvs
ENV VIRTUALENV_PATH /virtualenv

# Upgrade system and install dependencies
# NB: the date is a tip to bypass cache and retrigger the upgrade when needed
# It may well be possible that the build is newer though, for instance if we
# triggered a "docker pull ubuntu" and invalidated the cache for the "FROM"
# above.
RUN apt-get update && apt-get dist-upgrade -y #2015-03-14
RUN apt-get install -y git curl wget sudo make autoconf python \
    python-dev python-pip openjdk-7-jre-headless

# NB: pip install pip has to be the last, else we get a "pip: command not found"
# for subsequent calls to pip ; further layers don't have this problem, pip is
# found in /usr/local/bin as it should be.
ADD docker/pip.conf /root/.pip/pip.conf
RUN pip install setuptools==15.2 && pip install -U pip

# Have to remove six, otherwise will have conflicts with pip installed six
RUN apt-get remove python-six -y

# 2- PREPARE APP
# --------------

# Install cdf requirements
#
# NB: here we install dependencies in order because each one needs the following to be
# installed so it can itself run its install steps. Crazy.
RUN mkdir -p /deps/botify-cdf

# debian dependencies
ADD packaging/debian.deps /deps/botify-cdf/
# for various reasons we don't want to install python related packages by deb
# they are handled by `pip`
RUN apt-get install -y $(grep -v '^python' /deps/botify-cdf/debian.deps)

# debian build dependencies
ADD packaging/debian_build.deps /deps/botify-cdf/
RUN apt-get install -y $(cat /deps/botify-cdf/debian_build.deps)

# For later so we can find mpi.h when installing packages
ENV C_INCLUDE_PATH /usr/lib/openmpi/include

# Judy
# The version in Ubuntu 14.04 doesn't work
RUN cd /tmp && wget http://ftp.fr.debian.org/debian/pool/main/j/judy/libjudydebian1_1.0.5-4_amd64.deb && dpkg -i libjudydebian1_1.0.5-4_amd64.deb

# python deps
#RUN pip install numpy==1.6.1 && pip install Cython==0.19.1 && pip install cffi==0.9.2
ADD packaging/python.deps /deps/botify-cdf/
# Fetch versions in the deps file
RUN bash -c 'set -eo pipefail; egrep "^(numpy|Cython|cffi)" /deps/botify-cdf/python.deps | xargs pip install'
RUN pip install -r /deps/botify-cdf/python.deps

# python test deps
ADD packaging/python_test.deps /deps/botify-cdf/
RUN pip install -r /deps/botify-cdf/python_test.deps

# 3- PREPARE pypy Environment
# ---------------------------
## Install pypy from a ppa to get a recent version.
RUN apt-get install -q -y software-properties-common
RUN apt-add-repository ppa:pypy/ppa && apt-get update && apt-get install -q -y pypy
## Install dependencies in a virtualenv
RUN mkdir -p ${VIRTUALENV_PATH}
RUN apt-get install -q -y python-virtualenv pypy-dev
RUN virtualenv -p pypy ${VIRTUALENV_PATH}/pypy
ADD packaging/pypy.deps /deps/botify-cdf/
ADD packaging/pypy_test.deps /deps/botify-cdf/
RUN ${VIRTUALENV_PATH}/pypy/bin/pip install cffi==0.9.2 && ${VIRTUALENV_PATH}/pypy/bin/pip install -r /deps/botify-cdf/pypy.deps && ${VIRTUALENV_PATH}/pypy/bin/pip install -r /deps/botify-cdf/pypy_test.deps

# 4- INSTALL
# -----------
RUN mkdir -p /code/botify-cdf/
WORKDIR /code/botify-cdf/
ADD . /code/botify-cdf/
## Install default CPython version
RUN python /code/botify-cdf/setup.py install

## Install cdf in pypy environment
RUN ${VIRTUALENV_PATH}/pypy/bin/pip install -I --no-deps -e /code/botify-cdf

ENTRYPOINT ["/bin/bash", "docker/run"]
