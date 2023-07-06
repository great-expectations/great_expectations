# To use a specific python version, replace the first line with the following,
# where 3.10 is the version you want to use (requires a `--rebuild`):
# FROM jupyter/minimal-notebook:python-3.10
FROM jupyter/minimal-notebook

WORKDIR /gx

COPY ./snowflake_example.ipynb ./

# snowflake-sqlalchemy connector is currently not compatible with sqlachemy 2.0.
# This line can be changed to `RUN pip install great_expectations[snowflake]` once they are compatible.
RUN pip install 'great_expectations[snowflake, sqlalchemy-less-than-2]'

# Uncomment this line to install GX from the develop branch (requires a `--rebuild`),
# or replace develop with the branch name you want to install from:
# RUN pip install git+https://github.com/great-expectations/great_expectations.git@develop
