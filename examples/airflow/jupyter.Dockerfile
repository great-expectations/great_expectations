FROM jupyter/minimal-notebook

WORKDIR /gx

COPY ./postgres_example.ipynb ./
COPY ./yellow_tripdata_sample_2019-01.csv ./

RUN pip install great_expectations[postgresql]
