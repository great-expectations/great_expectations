FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY ./postgres_example.ipynb ./

RUN pip install great_expectations[postgresql]

# Use this line to install GX from the develop branch,
# or replace develop with the branch name you want to install from:
RUN pip install git+https://github.com/great-expectations/great_expectations.git@develop
