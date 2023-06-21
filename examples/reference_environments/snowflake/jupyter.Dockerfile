FROM jupyter/minimal-notebook

WORKDIR /gx

COPY ./snowflake_example.ipynb ./

RUN pip install great_expectations[snowflake, sqlalchemy-less-than-2]
