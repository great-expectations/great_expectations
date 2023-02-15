#!/usr/bin/env bash

# Create BigQuery tables for running performance tests against.
#
# This script is intended to be run once, and the tables are intended to be reused without modification by the performance tests.
# Note that this script takes about 5 minutes to run.

set -exu

dataset=${GE_TEST_BIGQUERY_PEFORMANCE_DATASET:-performance_ci}
bq mk ${GE_TEST_BIGQUERY_PROJECT}:${dataset}

for i in {1..100}
do
  bq load --skip_leading_rows=1 \
    ${GE_TEST_BIGQUERY_PROJECT}:${dataset}.taxi_trips_${i} \
    ../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv \
    bigquery_schema.json
done
