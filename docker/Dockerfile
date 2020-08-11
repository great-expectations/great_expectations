ARG PYTHON_DOCKER_TAG

FROM python:${PYTHON_DOCKER_TAG}

ARG GE_EXTRA_DEPS="spark,sqlalchemy,redshift,s3,gcp,snowflake"

ENV PYTHONIOENCODING utf-8
ENV LANG C.UTF-8
ENV HOME /root
ENV PATH /usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${HOME}/.local/bin
# Path where the root of the GE project will be expected
ENV GE_HOME /usr/app/great_expectations

LABEL maintainer="great-expectations"
LABEL org.opencontainers.image.title="Great Expectations"
LABEL org.opencontainers.image.description="Great Expectations. Always know what to expect from your data."
LABEL org.opencontainers.image.version=${GE_VERSION}
LABEL org.opencontainers.image.created=${CREATED}
LABEL org.opencontainers.image.url="https://github.com/great-expectations/great_expectations"
LABEL org.opencontainers.image.documentation="https://github.com/great-expectations/great_expectations"
LABEL org.opencontainers.image.source="https://github.com/great-expectations/great_expectations"

# The interface to Great Expectations
# is a CLI, therefore we use root to
# avoid common permissioning pitfalls
USER root

COPY . /tmp/great_expectations_install

RUN mkdir -p /usr/app ${HOME} && \
    cd /tmp/great_expectations_install && \
    pip install .[${GE_EXTRA_DEPS}] && \
    rm -rf /tmp/great_expectations_install

WORKDIR ${GE_HOME}

ENTRYPOINT ["great_expectations"]
CMD ["--help"]
