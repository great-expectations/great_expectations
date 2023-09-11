# Example Configuration for GCP Deployment Guide

This directory contains an example configuration for [Using Great Expectations with Google Cloud Platform and BigQuery](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/fluent/database/connect_sql_source_data)

It contains a `great_expectations.yml` with a configuration for Metadata stores in GCS, and a Datadocs store in GCS, both with placeholder values. 

The folder also contains the following Checkpoints :
    - `bigquery_checkpoint.yml`
    - `gcs_checkpoint.yml`

And DAG files, which are intended to be used with Cloud Composer.
- `ge_checkpoint_bigquery.py`
- `ge_checkpoint_gcs.py`

For more information on these configurations, please refer to the Deployment Guide. 
