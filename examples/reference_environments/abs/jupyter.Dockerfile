FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY abs_example.ipynb ./
COPY abs_example_abs_stores.ipynb ./

RUN pip install great_expectations[azure]
