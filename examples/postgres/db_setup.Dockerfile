FROM python

WORKDIR /gx

COPY ./load_data.py ./
COPY ./yellow_tripdata_sample_2019-01.csv ./

RUN pip install pandas sqlalchemy
