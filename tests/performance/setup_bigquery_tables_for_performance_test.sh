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
  bq cp bigquery-public-data:austin_bikeshare.bikeshare_trips ${GE_TEST_BIGQUERY_PROJECT}:${dataset}.bikeshare_trips_${i}
done
