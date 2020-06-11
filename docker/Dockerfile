ARG PYTHON_DOCKER_TAG

FROM python:${PYTHON_DOCKER_TAG} as base

LABEL maintainer="great-expectations"

COPY . /usr/app/great_expectations_install

RUN cd /usr/app/great_expectations_install && \
    python setup.py install

FROM base

ENV PYTHONIOENCODING=utf-8
ENV LANG C.UTF-8

RUN useradd -mU great-expectations-user

USER great-expectations-user

ENTRYPOINT ["great_expectations"]
