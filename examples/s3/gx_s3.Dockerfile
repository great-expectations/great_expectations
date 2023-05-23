FROM python:3.9

WORKDIR /gx

COPY ./run_quickstart.py ./

RUN pip install great_expectations[s3]
# TEMPORARY PIN:
RUN pip install typing_extensions==4.5.0
