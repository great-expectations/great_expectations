FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY s3_public_nyc_tlc_bucket_example.ipynb ./

RUN pip install great_expectations[s3]
