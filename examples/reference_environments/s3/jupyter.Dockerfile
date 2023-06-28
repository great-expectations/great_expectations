FROM jupyter/minimal-notebook:python-3.10

WORKDIR /gx

COPY s3_public_nyc_tlc_bucket_example.ipynb ./
COPY s3_public_nyc_tlc_bucket_s3_stores_example.ipynb ./

RUN pip install great_expectations[s3]

# TODO: This is temporary to enable changing stores until PR 8194 is merged (https://github.com/great-expectations/great_expectations/pull/8194), it should be removed before the code changes in branch `f/dx-609/enable_using_different_store_backends_in_ref_envs` are merged:
RUN pip install git+https://github.com/great-expectations/great_expectations.git@f/dx-609/enable_using_different_store_backends_in_ref_envs
