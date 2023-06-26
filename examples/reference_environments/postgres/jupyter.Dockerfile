FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY ./postgres_example.ipynb ./

RUN pip install great_expectations[postgresql]
