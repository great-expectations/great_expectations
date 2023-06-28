FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY ./postgres_example.ipynb ./
COPY ./postgres_example_postgres_stores.ipynb ./

RUN pip install great_expectations[postgresql]

# TODO: This is temporary for debugging, should be removed before merge:
RUN pip install git+https://github.com/great-expectations/great_expectations.git@f/dx-609/enable_using_different_store_backends_in_ref_envs
