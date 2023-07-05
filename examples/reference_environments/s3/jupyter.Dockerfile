FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY s3_public_nyc_tlc_bucket_example.ipynb ./
COPY s3_public_nyc_tlc_bucket_s3_stores_example.ipynb ./

RUN pip install great_expectations[s3]

# Uncomment this line to install GX from the develop branch (requires a `--rebuild`),
# or replace develop with the branch name you want to install from:
# RUN pip install git+https://github.com/great-expectations/great_expectations.git@develop
