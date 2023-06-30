FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY ./postgres_example.ipynb ./
COPY ./postgres_example_postgres_stores.ipynb ./

RUN pip install great_expectations[postgresql]
