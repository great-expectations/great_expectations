FROM jupyter/minimal-notebook

WORKDIR /gx

# TODO: Is creating a great_expectations folder necessary?
RUN mkdir -p great_expectations

COPY ./postgres_example.ipynb ./
COPY ./yellow_tripdata_sample_2019-01.csv ./

RUN pip install great_expectations[postgresql]
