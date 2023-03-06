# Development Dockerfile; see README.rst
FROM python:3.7
MAINTAINER labs@botify.com

RUN curl -s https://bootstrap.pypa.io/get-pip.py | python - && pip install -U setuptools

WORKDIR /code

ADD setup.py pyproject.toml ./
COPY . ./
RUN pip install -e .[dev]
