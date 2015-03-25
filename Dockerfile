FROM ubuntu:14.04

# 1- PREPARE SYSTEM
# -----------------

# This forces python to have stdout/sdtin/stderr totally unbuffered, which is
# better for logs especially, and recommended in most Django Dockerfiles I've
# seen.
ENV PYTHONUNBUFFERED 1

# Don't ask for interactive confirmation on install/updates
ENV DEBIAN_FRONTEND noninteractive

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
RUN pip install setuptools==9.1 && pip install -U pip

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

# python deps
RUN pip install numpy==1.6.1 && pip install numexpr==2.1 Cython==0.19.1
ADD packaging/python.deps /deps/botify-cdf/
RUN pip install -r /deps/botify-cdf/python.deps

# python test deps
ADD packaging/python_test.deps /deps/botify-cdf/
RUN pip install -r /deps/botify-cdf/python_test.deps

# 3- INSTALL
# -----------
RUN mkdir -p /code/botify-cdf/
WORKDIR /code/botify-cdf/
ADD . /code/botify-cdf/
RUN python /code/botify-cdf/setup.py install

ENTRYPOINT ["/bin/bash", "docker/run"]