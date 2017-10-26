# Development Dockerfile; see README.rst
FROM python:2.7
MAINTAINER labs@botify.com

RUN curl -s https://bootstrap.pypa.io/get-pip.py | python -
RUN mkdir /code

ADD . /code/simpleflow

WORKDIR /code/simpleflow

RUN pip install .
RUN pip install -r requirements-dev.txt
RUN python setup.py develop
