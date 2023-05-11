FROM jupyter/minimal-notebook

WORKDIR /gx

RUN mkdir -p great_expectations

COPY ./yellow_tripdata_sample_2019-01.csv ./data/yellow_tripdata_sample_2019-01.csv
COPY ./quickstart.py ./

RUN pip install great_expectations
RUN python ./quickstart.py
