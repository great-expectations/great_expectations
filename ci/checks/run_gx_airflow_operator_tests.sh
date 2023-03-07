#!/bin/bash

# Run unit tests defined in the Great Expectations Airflow Operator

git clone git@github.com:astronomer/airflow-provider-great-expectations.git

cd airflow-provider-great-expectations

pip install -e '.[tests]'

pytest
