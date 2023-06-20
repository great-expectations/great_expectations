FROM jupyter/minimal-notebook

WORKDIR /gx

COPY ./snowflake_example.ipynb ./

# snowflake-sqlalchemy 1.4.7 requires sqlalchemy<2.0.0,>=1.4.0 
RUN pip install great_expectations[snowflake]
RUN pip install "sqlalchemy<2.0.0"
