FROM python:3.9-slim

WORKDIR /ge

COPY great_expectations/ great_expectations/

COPY data/Titanic.csv ./data/Titanic.csv

RUN pip install great_expectations
