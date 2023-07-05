FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

RUN mkdir -p gx_stores/data_docs/

COPY ./postgres_example.ipynb ./
COPY ./postgres_example_postgres_stores.ipynb ./

RUN pip install great_expectations[postgresql]

# Uncomment this line to install GX from the develop branch (requires a `--rebuild`),
# or replace develop with the branch name you want to install from:
# RUN pip install git+https://github.com/great-expectations/great_expectations.git@develop
