# Development Dockerfile; see README.rst
FROM python:3.7
MAINTAINER labs@botify.com

RUN curl -s https://bootstrap.pypa.io/get-pip.py | python - && pip install -U setuptools
RUN mkdir /code

ADD . /code/simpleflow

WORKDIR /code/simpleflow

RUN pip install -e .
RUN pip install -r requirements-dev.txt
