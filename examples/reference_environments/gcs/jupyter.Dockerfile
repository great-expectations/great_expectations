FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY gcs_public_nyc_tlc_bucket_example.ipynb ./
# bigquery connector is currently not compatible with sqlachemy 2.0.
# This line can be changed to `RUN pip install great_expectations[bigquery]` once they are compatible.
RUN pip install 'great_expectations[bigquery, sqlalchemy-less-than-2]'
