FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY abs_example.ipynb ./
COPY abs_example_abs_stores.ipynb ./

RUN pip install great_expectations[azure]

# Uncomment this line to install GX from the develop branch (requires a `--rebuild`),
# or replace develop with the branch name you want to install from:
# RUN pip install git+https://github.com/great-expectations/great_expectations.git@develop
