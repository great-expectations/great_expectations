ARG PYTHON_DOCKER_TAG

FROM python:${PYTHON_DOCKER_TAG}

ARG GE_EXTRA_DEPS="spark,sqlalchemy,redshift,s3,gcp"

ENV PYTHONIOENCODING utf-8
ENV LANG C.UTF-8
ENV HOME /home/great-expectations
ENV PATH /usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${HOME}/.local/bin
# Path where the root of the GE project will be expected
ENV GE_HOME ${HOME}/great_expectations

LABEL maintainer="great-expectations"
LABEL org.opencontainers.image.title="Great Expectations"
LABEL org.opencontainers.image.description="Great Expectations. Always know what to expect from your data."
LABEL org.opencontainers.image.version=${GE_VERSION}
LABEL org.opencontainers.image.created=${CREATED}
LABEL org.opencontainers.image.url="https://github.com/great-expectations/great_expectations"
LABEL org.opencontainers.image.documentation="https://github.com/great-expectations/great_expectations"
LABEL org.opencontainers.image.source="https://github.com/great-expectations/great_expectations"
RUN useradd -mU great-expectations -d ${HOME}

USER great-expectations

COPY --chown=great-expectations . ${HOME}/great_expectations_install

RUN cd ${HOME}/great_expectations_install && \
    pip install --user .[${GE_EXTRA_DEPS}] && \
    rm -rf ${HOME}/great_expectations_install

WORKDIR ${HOME}

ENTRYPOINT ["great_expectations"]
