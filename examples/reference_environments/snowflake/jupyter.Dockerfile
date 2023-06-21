FROM jupyter/minimal-notebook

WORKDIR /gx

COPY ./snowflake_example.ipynb ./

# snowflake-sqlalchemy connector is currently not compatible with sqlachemy 2.0. 
# This line can be changed to `RUN pip install great_expectations[snowflake]` once they are compatible.
RUN pip install great_expectations[snowflake, sqlalchemy-less-than-2]
