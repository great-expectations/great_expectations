FROM jupyter/minimal-notebook

WORKDIR /gx

COPY ./postgres_example.ipynb ./

RUN pip install great_expectations[postgresql]
