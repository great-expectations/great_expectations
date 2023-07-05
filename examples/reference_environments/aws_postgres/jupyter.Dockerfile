FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY ./aws_postgres_example.ipynb ./
COPY ./aws_postgres_example_aws_stores.ipynb ./

RUN pip install 'great_expectations[s3, postgresql]'
