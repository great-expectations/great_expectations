ARG PYTHON_DOCKER_TAG

FROM python:${PYTHON_DOCKER_TAG}

ARG GE_EXTRA_DEPS="spark,sqlalchemy,redshift,s3"

ENV PYTHONIOENCODING utf-8
ENV LANG C.UTF-8
ENV PATH /usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${GE_HOME}/.local/bin

# Path where the root of the GE project will be expected
ENV GE_HOME /home/great-expectations/great_expectations

LABEL maintainer="great-expectations"

RUN useradd -mU great-expectations -d ${GE_HOME}

USER great-expectations

COPY --chown=great-expectations . ${GE_HOME}/great_expectations_install

RUN cd ${GE_HOME}/great_expectations_install && \
    pip install --user .[${GE_EXTRA_DEPS}] && \
    rm -rf ${GE_HOME}/great_expectations_install

WORKDIR /home/great-expectations

ENTRYPOINT ["great_expectations"]
