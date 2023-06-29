FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY abs_example.ipynb ./

RUN pip install great_expectations[azure]
