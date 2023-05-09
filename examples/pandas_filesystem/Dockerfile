FROM python:3.9-slim

WORKDIR /gx


#COPY ./examples/data/Titanic.csv ./data/Titanic.csv
##COPY ../../tests/test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv ./data/yellow_tripdata_sample_2019-01.csv
#COPY examples/data/yellow_tripdata_sample_2019-01.csv ./data/yellow_tripdata_sample_2019-01.csv
#COPY examples/pandas_filesystem/quickstart.py ./quickstart.py
COPY ./examples/pandas_filesystem/quickstart.py ./

RUN pip install great_expectations
#RUN python ./quickstart.py
