# To use a specific python version, replace the first line with the following,
# where 3.10 is the version you want to use (requires a `--rebuild`):
# FROM jupyter/minimal-notebook:python-3.10
FROM jupyter/minimal-notebook

WORKDIR /gx

COPY gcs_public_nyc_tlc_bucket_example.ipynb ./
COPY gcs_public_nyc_tlc_bucket_gcs_stores_example.ipynb ./

RUN pip install 'great_expectations[bigquery]'

# Uncomment this line to install GX from the develop branch (requires a `--rebuild`),
# or replace develop with the branch name you want to install from:
# RUN pip install git+https://github.com/great-expectations/great_expectations.git@develop
