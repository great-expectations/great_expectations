ARG PYTHON_DOCKER_TAG

FROM python:${PYTHON_DOCKER_TAG}

LABEL maintainer="great-expectations"

ENV PYTHONIOENCODING=utf-8
ENV LANG C.UTF-8

RUN useradd -mU great-expectations-user

USER great-expectations-user

ENTRYPOINT great_expectations
